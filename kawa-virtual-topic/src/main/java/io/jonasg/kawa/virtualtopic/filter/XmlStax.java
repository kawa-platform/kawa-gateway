package io.jonasg.kawa.virtualtopic.filter;

import org.jspecify.annotations.Nullable;
import tools.jackson.dataformat.xml.XmlFactory;

import javax.xml.stream.Location;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;

/// The StAX (Woodstox) factories [XmlPayloadDecoder] and [XmlPayloadEncoder] share, taken from
/// Jackson's [XmlFactory] and configured once. StAX factories are thread-safe once configured;
/// every call creates its own reader or writer.
final class XmlStax {

    /// Not namespace aware, so names are kept as written (`soap:Body`) and `xmlns` declarations
    /// are plain attributes. Text and CDATA are coalesced into one event. DTDs and external
    /// entities are off: a record payload is untrusted, so a DOCTYPE is refused instead of
    /// resolved (no XXE, no entity expansion).
    static final XMLInputFactory INPUT;

    static final XMLOutputFactory OUTPUT;

    static {
        var factory = new XmlFactory();
        INPUT = factory.getXMLInputFactory();
        INPUT.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false);
        INPUT.setProperty(XMLInputFactory.IS_COALESCING, true);
        INPUT.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        INPUT.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        INPUT.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, true);
        OUTPUT = factory.getXMLOutputFactory();
        OUTPUT.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, false);
    }

    private XmlStax() {
    }

    /// ` at line L, column C` for a StAX failure's location, empty when it has none. Failure
    /// messages carry this instead of the parser's own message, which can quote the payload.
    static String at(@Nullable Location location) {
        return location == null || location.getLineNumber() < 0
                ? ""
                : " at line " + location.getLineNumber() + ", column " + location.getColumnNumber();
    }
}
