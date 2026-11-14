package io.jonasg.kawa.virtualtopic.filter;

import dev.cel.common.values.CelByteString;
import dev.cel.common.values.NullValue;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/// Encodes a value tree (see [PayloadEncoder]) as the content of an XML root element: the
/// inverse of [XmlPayloadDecoder], with the same mapping.
///
/// | Value tree                        | XML                                                |
/// |-----------------------------------|----------------------------------------------------|
/// | `Map`                             | child elements; `@name` keys become attributes (written first), `#text` its text |
/// | `List` (as a field value)         | the element repeated once per item                 |
/// | `String`                          | text                                               |
/// | `Long`, `Double`, `Boolean`       | their text (they decode back as `String`)          |
/// | [CelByteString]                   | base64 text                                        |
/// | `NullValue`                       | an empty element (decodes back as `""`)            |
///
/// Output is UTF-8 without an XML declaration; text and attribute values are escaped. Names are
/// written as given, prefix included, so `@xmlns:p` keys carry namespace declarations through.
///
/// Refused with a [PayloadEncodeException], so every output decodes again: a key that is not a
/// valid XML name, an attribute or `#text` that is not a scalar, `#text` next to child elements
/// (mixed content), a list inside a list, a list as the whole value, and text with characters
/// XML 1.0 cannot carry.
///
/// A one-element list is written as a single element, so it decodes back as a single value.
public final class XmlPayloadEncoder implements PayloadEncoder {

    private static final Pattern NAME = Pattern.compile("[\\p{L}_][\\p{L}\\p{N}_.\\-]*(:[\\p{L}_][\\p{L}\\p{N}_.\\-]*)?");
    private static final String TEXT = "#text";

    private final String root;

    /// @param root the root element name
    public XmlPayloadEncoder(String root) {
        this.root = requireName(root, root);
    }

    @Override
    public byte[] encode(Object value) {
        if (value instanceof List<?>) {
            throw new PayloadEncodeException("cannot encode an array as XML: a document has a single root element");
        }
        var out = new ByteArrayOutputStream();
        try {
            XMLStreamWriter writer = XmlStax.OUTPUT.createXMLStreamWriter(out, StandardCharsets.UTF_8.name());
            element(writer, root, value, root);
            writer.writeEndDocument();
            writer.close();
        } catch (XMLStreamException e) {
            throw new PayloadEncodeException("value cannot be written as XML: " + e.getMessage(), e);
        }
        return out.toByteArray();
    }

    private static void element(XMLStreamWriter writer, String name, Object value, String path) throws XMLStreamException {
        switch (value) {
            case Map<?, ?> map -> object(writer, name, map, path);
            case NullValue _ -> writer.writeEmptyElement(name);
            case List<?> _ -> throw new PayloadEncodeException("cannot encode a list inside a list at " + path + " as XML");
            default -> {
                writer.writeStartElement(name);
                writer.writeCharacters(text(value, path));
                writer.writeEndElement();
            }
        }
    }

    private static void object(XMLStreamWriter writer, String name, Map<?, ?> map, String path) throws XMLStreamException {
        writer.writeStartElement(name);
        boolean hasChildren = false;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = key(entry.getKey(), path);
            if (key.startsWith("@")) {
                writer.writeAttribute(requireName(key.substring(1), path + "/" + key), text(entry.getValue(), path + "/" + key));
            } else if (!key.equals(TEXT)) {
                hasChildren = true;
            }
        }
        Object text = map.get(TEXT);
        if (map.containsKey(TEXT)) {
            if (hasChildren) {
                throw new PayloadEncodeException("cannot encode '#text' next to child elements (mixed content) at " + path);
            }
            writer.writeCharacters(text(text, path + "/" + TEXT));
        }
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = (String) entry.getKey();
            if (key.startsWith("@") || key.equals(TEXT)) {
                continue;
            }
            String childPath = path + "/" + key;
            requireName(key, childPath);
            if (entry.getValue() instanceof List<?> items) {
                for (Object item : items) {
                    element(writer, key, item, childPath);
                }
            } else {
                element(writer, key, entry.getValue(), childPath);
            }
        }
        writer.writeEndElement();
    }

    private static String key(Object key, String path) {
        if (key instanceof String name) {
            return name;
        }
        throw new PayloadEncodeException("XML names must be strings, got " + key + " at " + path);
    }

    private static String text(Object value, String path) {
        String text = switch (value) {
            case String string -> string;
            case Long _, Double _, Boolean _ -> value.toString();
            case CelByteString bytes -> Base64.getEncoder().encodeToString(bytes.toByteArray());
            case null -> throw new PayloadEncodeException("value tree holds a Java null at " + path + ", expected NullValue");
            default -> throw new PayloadEncodeException("cannot encode " + kind(value) + " at " + path + " as XML text");
        };
        text.codePoints().filter(c -> !isXmlChar(c)).findFirst().ifPresent(c -> {
            throw new PayloadEncodeException("text at " + path + " holds U+" + String.format("%04X", c) + ", which XML cannot carry");
        });
        return text;
    }

    private static String kind(Object value) {
        return switch (value) {
            case Map<?, ?> _ -> "an object";
            case List<?> _ -> "an array";
            case NullValue _ -> "null";
            default -> value.getClass().getSimpleName();
        };
    }

    private static boolean isXmlChar(int c) {
        return c == 0x9 || c == 0xA || c == 0xD || (c >= 0x20 && c <= 0xD7FF) || (c >= 0xE000 && c <= 0xFFFD) || c >= 0x10000;
    }

    private static String requireName(String name, String path) {
        if (!NAME.matcher(name).matches()) {
            throw new PayloadEncodeException("'" + name + "' at " + path + " is not a valid XML name");
        }
        return name;
    }
}
