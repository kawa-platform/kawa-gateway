package io.jonasg.kawa.mcp.peekrecords;

import dev.cel.common.values.CelByteString;
import dev.cel.common.values.NullValue;
import io.jonasg.kawa.virtualtopic.filter.AvroPayloadDecoder;
import io.jonasg.kawa.virtualtopic.filter.AvroSchemaResolver;
import io.jonasg.kawa.virtualtopic.filter.JsonPayloadDecoder;
import io.jonasg.kawa.virtualtopic.filter.PayloadDecodeException;
import io.jonasg.kawa.virtualtopic.filter.SchemaRegistryUnavailableException;
import io.jonasg.kawa.virtualtopic.filter.XmlPayloadDecoder;
import org.jspecify.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/// Decodes record keys and values for `peek_records`, and searches them. Values decode with the gateway's own
/// payload decoders (the ones virtual topic filters use), so JSON, XML and Avro read the same here as there;
/// XML is parsed without DTDs or external entities.
///
/// Detection ([PayloadFormat#AUTO]) is per record and conservative, in this order:
///
/// 1. the schema registry wire format (magic byte `0`, a 4-byte schema id) whose schema id the registry knows: Avro
/// 2. a JSON object or array that parses: JSON
/// 3. an XML document that parses: XML
/// 4. valid UTF-8 without control characters (other than tab and line breaks): text
/// 5. anything else is left unidentified
///
/// A payload that only looks like a format (`{` that does not parse) falls through to the next check.
final class Payloads {

    private static final JsonPayloadDecoder JSON = new JsonPayloadDecoder();
    private static final XmlPayloadDecoder XML = new XmlPayloadDecoder(null);

    private Payloads() {
    }

    /// The outcome of decoding one value.
    ///
    /// @param format the format it was decoded in, or detected as; `null` when detection found none
    /// @param status `decoded`, `failed` (not valid in `format`) or `unidentified` (detection found no format)
    /// @param value  the decoded value as plain JSON values, when decoded
    /// @param error  why it did not decode
    record Decoded(@Nullable String format, String status, @Nullable Object value, @Nullable String error) {

        static Decoded ok(PayloadFormat format, @Nullable Object value) {
            return new Decoded(format.label(), "decoded", value, null);
        }

        static Decoded failed(PayloadFormat format, String error) {
            return new Decoded(format.label(), "failed", null, error);
        }

        boolean decoded() {
            return "decoded".equals(status);
        }
    }

    /// Decodes `bytes` in `format`, detecting it per record for [PayloadFormat#AUTO].
    ///
    /// @param avroSchemas the schema registry of the topic's cluster, `null` when it has none
    static Decoded decode(byte[] bytes, PayloadFormat format, @Nullable AvroSchemaResolver avroSchemas) {
        return switch (format) {
            case AUTO -> detect(bytes, avroSchemas);
            case JSON -> decodeWith(format, () -> JSON.decode(ByteBuffer.wrap(bytes)));
            case XML -> decodeWith(format, () -> XML.decode(ByteBuffer.wrap(bytes)));
            case AVRO -> avroSchemas == null
                    ? Decoded.failed(format, "no schema registry is configured for the cluster of this topic")
                    : decodeWith(format, () -> new AvroPayloadDecoder(avroSchemas).decode(ByteBuffer.wrap(bytes)));
            case STRING -> {
                String text = utf8(bytes);
                yield text == null ? Decoded.failed(format, "value is not valid UTF-8") : Decoded.ok(format, text);
            }
            case RAW -> new Decoded(format.label(), "not_decoded", null, null);
        };
    }

    private static Decoded detect(byte[] bytes, @Nullable AvroSchemaResolver avroSchemas) {
        if (bytes.length >= 5 && bytes[0] == 0) {
            int schemaId = ByteBuffer.wrap(bytes, 1, 4).getInt();
            if (avroSchemas == null) {
                return unidentified("value starts like the schema registry Avro wire format (schema id " + schemaId
                        + "), but no schema registry is configured for the cluster of this topic");
            }
            try {
                avroSchemas.resolve(schemaId);
            } catch (RuntimeException e) {
                return unidentified("value starts like the schema registry Avro wire format, but schema id " + schemaId
                        + " could not be resolved: " + PartitionScan.message(e));
            }
            return decodeWith(PayloadFormat.AVRO, () -> new AvroPayloadDecoder(avroSchemas).decode(ByteBuffer.wrap(bytes)));
        }
        String text = utf8(bytes);
        if (text == null) {
            return unidentified("value is binary: not schema registry Avro, JSON, XML or UTF-8 text");
        }
        String trimmed = text.strip();
        if (trimmed.startsWith("﻿")) {
            trimmed = trimmed.substring(1).strip();
        }
        if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
            var json = decodeWith(PayloadFormat.JSON, () -> JSON.decode(ByteBuffer.wrap(bytes)));
            if (json.decoded()) {
                return json;
            }
        }
        if (trimmed.startsWith("<")) {
            var xml = decodeWith(PayloadFormat.XML, () -> XML.decode(ByteBuffer.wrap(bytes)));
            if (xml.decoded()) {
                return xml;
            }
        }
        if (printable(text)) {
            return Decoded.ok(PayloadFormat.STRING, text);
        }
        return unidentified("value is valid UTF-8 but holds control characters, so it is not taken for text");
    }

    private static Decoded unidentified(String error) {
        return new Decoded(null, "unidentified", null, error);
    }

    private interface Decoder {
        Object decode();
    }

    private static Decoded decodeWith(PayloadFormat format, Decoder decoder) {
        try {
            return Decoded.ok(format, plain(decoder.decode()));
        } catch (SchemaRegistryUnavailableException e) {
            return Decoded.failed(format, "the schema registry is unavailable: " + PartitionScan.message(e));
        } catch (PayloadDecodeException e) {
            return Decoded.failed(format, e.getMessage());
        } catch (RuntimeException e) {
            return Decoded.failed(format, PartitionScan.message(e));
        }
    }

    /// The decoder's value tree as plain JSON values: CEL's null becomes `null`, bytes become Base64.
    static @Nullable Object plain(@Nullable Object value) {
        return switch (value) {
            case null -> null;
            case NullValue _ -> null;
            case CelByteString bytes -> Base64.getEncoder().encodeToString(bytes.toByteArray());
            case Map<?, ?> map -> {
                Map<String, @Nullable Object> copy = new LinkedHashMap<>();
                map.forEach((key, entry) -> copy.put(String.valueOf(key), plain(entry)));
                yield copy;
            }
            case List<?> list -> {
                List<@Nullable Object> copy = new ArrayList<>(list.size());
                list.forEach(element -> copy.add(plain(element)));
                yield copy;
            }
            default -> value;
        };
    }

    /// Whether any scalar in `value`, at any depth, contains `needle`, ignoring case. Field names are not searched.
    static boolean contains(@Nullable Object value, String needle) {
        return switch (value) {
            case null -> false;
            case Map<?, ?> map -> map.values().stream().anyMatch(entry -> contains(entry, needle));
            case List<?> list -> list.stream().anyMatch(element -> contains(element, needle));
            default -> lowerCase(String.valueOf(value)).contains(lowerCase(needle));
        };
    }

    static String lowerCase(String text) {
        return text.toLowerCase(Locale.ROOT);
    }

    /// `bytes` as UTF-8 text, or `null` when they are not valid UTF-8.
    static @Nullable String utf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    /// Whether `text` holds no control characters other than tab, line feed and carriage return.
    static boolean printable(String text) {
        return text.chars().noneMatch(c -> Character.isISOControl(c) && c != '\t' && c != '\n' && c != '\r');
    }
}
