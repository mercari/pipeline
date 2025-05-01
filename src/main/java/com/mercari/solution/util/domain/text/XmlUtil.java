package com.mercari.solution.util.domain.text;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;

public class XmlUtil {

    public static Document createDocument(final String root) {
        final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            return factory.newDocumentBuilder()
                    .getDOMImplementation()
                    .createDocument("", root, null);
        } catch (ParserConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String toString(final Document document) {
        final StringWriter writer = new StringWriter();
        try {
            final TransformerFactory factory = TransformerFactory.newInstance();
            final Transformer transformer = factory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(OutputKeys.METHOD, "xml");
            transformer.setOutputProperty("{http://xml.apache.org/xalan}indent-amount", "2");
            transformer.transform(new DOMSource(document), new StreamResult(writer));
            return writer.toString();
        } catch (TransformerException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Document parseJson(final JsonObject jsonObject) {
        final Document document = XmlUtil.createDocument("schema");
        ((Element)document.getFirstChild()).setAttribute("name", "content");
        ((Element)document.getFirstChild()).setAttribute("version", "1.6");

        final Element root = document.getDocumentElement();
        final Element content = parseJson(document, "schema", jsonObject);
        root.appendChild(content);
        return document;
    }

    private static Element parseJson(final Document document, final String name, final JsonElement jsonElement) {
        if (jsonElement.isJsonPrimitive()) {
            final Element element = document.createElement(name);
            element.setTextContent(jsonElement.getAsString());
            return element;
        } else if (jsonElement.isJsonObject()) {
            if(jsonElement.getAsJsonObject().has("tag")) {
                final String tag = jsonElement.getAsJsonObject().get("tag").getAsString();
                final Element element = document.createElement(tag);
                for (final Map.Entry<String, JsonElement> entry : jsonElement.getAsJsonObject().entrySet()) {
                    if("tag".equals(entry.getKey())) {
                        continue;
                    }
                    if(entry.getValue().isJsonPrimitive()) {
                        element.setAttribute(entry.getKey(), entry.getValue().getAsString());
                    } else if(entry.getValue().isJsonObject()) {
                        element.appendChild(parseJson(document, entry.getKey(), entry.getValue()));
                    } else if(entry.getValue().isJsonArray()) {
                        element.appendChild(parseJson(document, entry.getKey(), entry.getValue()));
                    }
                }
                return element;
            } else {
                final Element element = document.createElement(name);
                for (final Map.Entry<String, JsonElement> entry : jsonElement.getAsJsonObject().entrySet()) {
                    element.appendChild(parseJson(document, entry.getKey(), entry.getValue()));
                }
                return element;
            }
        } else if(jsonElement.isJsonArray()) {
            final Element element = document.createElement(name);
            for (final JsonElement childElement : jsonElement.getAsJsonArray()) {
                final Element element1 = parseJson(document, name, childElement);
                element.appendChild(element1);
            }
            return element;
        } else {
            throw new IllegalArgumentException();
        }
    }

    private static Document parse(String xmlStr) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            InputSource is = new InputSource(new StringReader(xmlStr));
            Document document = builder.parse(is);
            Element element = (Element) document.getFirstChild();
            element.setAttribute("id", "1");
            String result = document.toString();
            System.out.println(result);
            return document;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

    }


}
