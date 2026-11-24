package io.jonasg.kawa.virtualtopic.filter;

import org.jspecify.annotations.Nullable;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/// Decodes an XML payload into a value tree (see [PayloadDecoder]): the root element's content.
///
/// ```
/// <order id="7" xmlns:x="urn:x">
///   <x:total currency="EUR">12.50</x:total>
///   <line>A</line><line>B</line>
///   <note/>
/// </order>
/// ```
/// decodes to `{"@id": "7", "@xmlns:x": "urn:x", "x:total": {"@currency": "EUR", "#text": "12.50"},
/// "line": ["A", "B"], "note": ""}`.
///
/// | XML                                         | Value tree                                         |
/// |---------------------------------------------|----------------------------------------------------|
/// | element with only text, or empty            | `String` (`""` for `<a/>`)                         |
/// | element with attributes or child elements   | `Map`: `@name` per attribute, child element names, `#text` for its text |
/// | repeated sibling element                    | `List`, in document order                          |
/// | CDATA section                               | text                                               |
/// | `xmlns`, `xmlns:p` declaration              | an `@xmlns`, `@xmlns:p` attribute                  |
///
/// XML has no types, so every scalar is a `String` (`<zip>007</zip>` stays `"007"`; CEL casts
/// with `int(value.qty)`). Names keep their prefix as written, so namespaces survive a round
/// trip through [XmlPayloadEncoder]. Like [JsonPayloadDecoder], an absent field reads as `null`.
///
/// Rejected, because the value tree cannot represent them: mixed content (text next to child
/// elements), a repeated element interleaved with others (`<a/><b/><a/>`), any DOCTYPE, and a
/// root other than the configured one.
///
/// Lost, because they carry no data: comments, processing instructions, the XML declaration,
/// whitespace-only text between elements, whether text was CDATA, and `\r\n` line endings
/// (normalized to `\n` by every XML parser). A single occurrence of an element reads as a value,
/// not a one-element list.
public final class XmlPayloadDecoder implements PayloadDecoder {

    private final @Nullable String root;

    /// @param root the required root element name, or `null` to accept any root
    public XmlPayloadDecoder(@Nullable String root) {
        this.root = root;
    }

    @Override
    public Object decode(ByteBuffer payload) {
        if (payload == null || !payload.hasRemaining()) {
            throw new PayloadDecodeException("record value is empty, expected XML");
        }
        byte[] bytes = new byte[payload.remaining()];
        payload.duplicate().get(bytes);
        XMLStreamReader reader = null;
        try {
            reader = XmlStax.INPUT.createXMLStreamReader(new ByteArrayInputStream(bytes));
            String name = toRoot(reader);
            if (root != null && !root.equals(name)) {
                throw new PayloadDecodeException("record value has root element <" + name + ">, expected <" + root + ">");
            }
            Object value = element(reader, name);
            while (reader.hasNext()) {
                rejectDtd(reader.next());
            }
            return value;
        } catch (XMLStreamException | RuntimeException e) {
            if (e instanceof PayloadDecodeException decodeException) {
                throw decodeException;
            }
            throw new PayloadDecodeException("record value is not valid XML"
                    + (e instanceof XMLStreamException stream ? XmlStax.at(stream.getLocation()) : " (" + e.getClass().getSimpleName() + ")"), e);
        } finally {
            close(reader);
        }
    }

    /// Advances to the root element and returns its name.
    private static String toRoot(XMLStreamReader reader) throws XMLStreamException {
        while (reader.hasNext()) {
            int event = reader.next();
            rejectDtd(event);
            if (event == XMLStreamConstants.START_ELEMENT) {
                return name(reader.getPrefix(), reader.getLocalName());
            }
        }
        throw new PayloadDecodeException("record value has no root element, expected XML");
    }

    /// Reads the element the reader is positioned on, up to and including its end tag.
    private static Object element(XMLStreamReader reader, String path) throws XMLStreamException {
        Map<String, Object> fields = new NullDefaultingMap();
        for (int i = 0; i < reader.getAttributeCount(); i++) {
            fields.put("@" + name(reader.getAttributePrefix(i), reader.getAttributeLocalName(i)), reader.getAttributeValue(i));
        }
        boolean hasAttributes = !fields.isEmpty();
        boolean hasChildren = false;
        var text = new StringBuilder();
        String previous = null;
        while (true) {
            int event = reader.next();
            switch (event) {
                case XMLStreamConstants.START_ELEMENT -> {
                    String child = name(reader.getPrefix(), reader.getLocalName());
                    String childPath = path + "/" + child;
                    Object value = element(reader, childPath);
                    addChild(fields, child, value, previous, childPath);
                    previous = child;
                    hasChildren = true;
                }
                case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA, XMLStreamConstants.SPACE -> text.append(reader.getText());
                case XMLStreamConstants.END_ELEMENT -> {
                    return value(fields, hasAttributes, hasChildren, text.toString(), path);
                }
                case XMLStreamConstants.ENTITY_REFERENCE ->
                        throw new PayloadDecodeException("record value refers to the undeclared entity &" + reader.getLocalName() + "; at " + path);
                default -> rejectDtd(event); // comments and processing instructions carry no data
            }
        }
    }

    private static void addChild(Map<String, Object> fields, String child, Object value, @Nullable String previous, String path) {
        if (!fields.containsKey(child)) {
            fields.put(child, value);
            return;
        }
        if (!child.equals(previous)) {
            throw new PayloadDecodeException("record value repeats <" + child + "> after other elements at " + path
                    + ", which cannot keep its order");
        }
        // An element itself never decodes to a list, so a list here holds earlier occurrences.
        if (fields.get(child) instanceof List<?> occurrences) {
            @SuppressWarnings("unchecked")
            List<Object> repeated = (List<Object>) occurrences;
            repeated.add(value);
        } else {
            List<Object> repeated = new ArrayList<>();
            repeated.add(fields.get(child));
            repeated.add(value);
            fields.put(child, repeated);
        }
    }

    private static Object value(Map<String, Object> fields, boolean hasAttributes, boolean hasChildren, String text, String path) {
        if (hasChildren && !text.isBlank()) {
            throw new PayloadDecodeException("record value has mixed content (text next to child elements) at " + path);
        }
        if (!hasAttributes && !hasChildren) {
            return text;
        }
        if (!hasChildren && !text.isEmpty()) {
            fields.put("#text", text);
        }
        return fields;
    }

    private static void rejectDtd(int event) {
        if (event == XMLStreamConstants.DTD) {
            throw new PayloadDecodeException("record value declares a DOCTYPE, which is not accepted");
        }
    }

    private static String name(@Nullable String prefix, String localName) {
        return prefix == null || prefix.isEmpty() ? localName : prefix + ":" + localName;
    }

    private static void close(@Nullable XMLStreamReader reader) {
        if (reader != null) {
            try {
                reader.close();
            } catch (XMLStreamException _) {
                // nothing to release beyond the in-memory input
            }
        }
    }
}
