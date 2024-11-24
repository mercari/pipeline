package com.mercari.solution.util.schema;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.domain.text.XmlUtil;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SolrSchemaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(SolrSchemaUtil.class);

    public static class SolrConfig implements Serializable {

        private String dataDir;
        private DirectoryFactory directoryFactory;
        private SchemaFactory schemaFactory;
        private CodecFactory codecFactory;
        private IndexConfig indexConfig;
        private List<Lib> libs;
        private String luceneMatchVersion;

        private List<RequestHandler> requestHandlers;
        private List<SearchComponent> searchComponents;
        private UpdateHandler updateHandler;


        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.directoryFactory != null) {
                errorMessages.addAll(this.directoryFactory.validate());
            }
            if(this.schemaFactory != null) {
                errorMessages.addAll(this.schemaFactory.validate());
            }
            if(this.codecFactory != null) {
                errorMessages.addAll(this.codecFactory.validate());
            }
            if(this.indexConfig != null) {
                errorMessages.addAll(this.indexConfig.validate());
            }
            if(this.libs != null) {
                for(int i=0; i<libs.size(); i++) {
                    errorMessages.addAll(libs.get(i).validate(i));
                }
            }
            if(this.requestHandlers != null) {
                for(int i=0; i<requestHandlers.size(); i++) {
                    errorMessages.addAll(requestHandlers.get(i).validate(i));
                }
            }
            if(this.searchComponents != null) {
                for(int i=0; i<searchComponents.size(); i++) {
                    errorMessages.addAll(searchComponents.get(i).validate(i));
                }
            }
            return errorMessages;
        }

        public void setDefaults() {

            if(this.dataDir == null) {
                this.dataDir = "${solr.data.dir:}";
            }

            // setup DirectoryFactory
            if(this.directoryFactory == null) {
                this.directoryFactory = new DirectoryFactory();
            }
            this.directoryFactory.setDefaults();

            // setup SchemaFactory
            if(this.schemaFactory == null) {
                this.schemaFactory = new SchemaFactory();
            }
            this.schemaFactory.setDefaults();

            // setup CodecFactory
            if(this.codecFactory == null) {
                this.codecFactory = new CodecFactory();
            }
            this.codecFactory.setDefaults();

            // setup IndexConfig
            if(this.indexConfig == null) {
                this.indexConfig = new IndexConfig();
            }
            this.indexConfig.setDefaults();

            // setup Libs
            if(this.libs == null) {
                this.libs = new ArrayList<>();
            } else {
                for(final Lib lib : this.libs) {
                    lib.setDefaults();
                }
            }

            // setup RequestHandlers
            if(this.requestHandlers == null) {
                this.requestHandlers = new ArrayList<>();
            }
            if(this.requestHandlers.size() == 0) {
                // set default request handler
                final RequestHandler defaultSearchHandler = new RequestHandler();
                defaultSearchHandler.name = "/select";
                defaultSearchHandler.className = "solr.SearchHandler";
                final Map<String, String> defaults = new HashMap<>();
                defaults.put("echoParams", "explicit");
                defaults.put("rows", "10");
                defaultSearchHandler.defaults = defaults;
                defaultSearchHandler.setDefaults();
                this.requestHandlers.add(defaultSearchHandler);
            }

            // setup UpdateHandler
            if(this.updateHandler == null) {
                this.updateHandler = new UpdateHandler();
                this.updateHandler.setDefaults();
            }

            // setup SearchComponents
            if(this.searchComponents == null) {
                this.searchComponents = new ArrayList<>();
            }

            if(this.luceneMatchVersion == null) {
                this.luceneMatchVersion = "9.0.0";
            }

        }

        public org.w3c.dom.Document createDocument() {
            final org.w3c.dom.Document document = XmlUtil.createDocument("config");
            final Element root = document.getDocumentElement();

            final Element luceneMatchVersion = document.createElement("luceneMatchVersion");
            luceneMatchVersion.setTextContent(this.luceneMatchVersion);
            root.appendChild(luceneMatchVersion);

            final Element dataDir = document.createElement("dataDir");
            dataDir.setTextContent(this.dataDir);
            root.appendChild(dataDir);

            final Element directoryFactory = this.directoryFactory.createElement(document);
            root.appendChild(directoryFactory);

            final Element schemaFactory = this.schemaFactory.createElement(document);
            root.appendChild(schemaFactory);

            final Element indexConfig = this.indexConfig.createElement(document);
            root.appendChild(indexConfig);

            final Element codecFactory = this.codecFactory.createElement(document);
            root.appendChild(codecFactory);

            for(final Lib lib : this.libs) {
                final Element libElm = lib.createElement(document);
                root.appendChild(libElm);
            }

            for(final RequestHandler requestHandler : this.requestHandlers) {
                final Element reqElem = requestHandler.createElement(document);
                root.appendChild(reqElem);
            }

            for(final SearchComponent searchComponent : this.searchComponents) {
                final Element scElem = searchComponent.createElement(document);
                root.appendChild(scElem);
            }

            return document;
        }

        private static class DirectoryFactory implements Serializable {

            private String className;
            private Boolean preload;

            public List<String> validate() {
                final List<String> errorMessages = new ArrayList<>();
                return errorMessages;
            }

            public void setDefaults() {
                if(this.className == null) {
                    //this.className = "solr.NRTCachingDirectoryFactory";
                    this.className = "solr.MMapDirectoryFactory";
                    // or solr.MMapDirectoryFactory or solr.NIOFSDirectoryFactory or org.apache.solr.core.RAMDirectoryFactory
                }
                if(this.preload == null) {
                    this.preload = true;
                }
            }

            public Element createElement(final Document document) {
                final Element directoryFactory = document.createElement("directoryFactory");
                directoryFactory.setAttribute("name", "DirectoryFactory");
                directoryFactory.setAttribute("class", this.className);
                if("solr.MMapDirectoryFactory".equals(this.className) || "solr.NIOFSDirectoryFactory".equals(this.className)) {
                    final Element preloadElm = document.createElement("bool");
                    preloadElm.setAttribute("name", "preload");
                    preloadElm.setTextContent(this.preload.toString());
                    directoryFactory.appendChild(preloadElm);
                }
                return directoryFactory;
            }
        }

        private class SchemaFactory implements Serializable {

            private String className;
            private Boolean mutable;
            private String managedSchemaResourceName;

            public List<String> validate() {
                final List<String> errorMessages = new ArrayList<>();
                return errorMessages;
            }

            public void setDefaults() {
                if(this.className == null) {
                    this.className = "ClassicIndexSchemaFactory"; // or ManagedIndexSchemaFactory
                }
                if(this.mutable == null) {
                    this.mutable = true;
                }
                if(this.managedSchemaResourceName == null) {
                    this.managedSchemaResourceName = "managed-schema.xml";
                }
            }

            public Element createElement(final Document document) {
                final Element schemaFactory = document.createElement("schemaFactory");
                schemaFactory.setAttribute("class", this.className);
                if("ManagedIndexSchemaFactory".equals(this.className)) {
                    final Element mutableElm = document.createElement("bool");
                    mutableElm.setAttribute("name", "mutable");
                    mutableElm.setTextContent(this.mutable.toString());
                    schemaFactory.appendChild(mutableElm);

                    final Element resourceNameElm = document.createElement("str");
                    resourceNameElm.setAttribute("name", "managedSchemaResourceName");
                    resourceNameElm.setTextContent(this.managedSchemaResourceName);
                    schemaFactory.appendChild(resourceNameElm);
                }
                return schemaFactory;
            }

        }

        private static class IndexConfig implements Serializable {

            private Integer ramBufferSizeMB;
            private Integer maxBufferedDocs;
            private Boolean useCompoundFile;
            private Integer ramPerThreadHardLimitMB;

            private String lockType;

            public List<String> validate() {
                final List<String> errorMessages = new ArrayList<>();
                return errorMessages;
            }

            public void setDefaults() {
                if(this.lockType == null) {
                    this.lockType = "none";
                }
            }

            public Element createElement(final Document document) {
                final Element indexConfig = document.createElement("indexConfig");
                if(this.ramBufferSizeMB != null) {
                    final Element ramBufferSizeMBElm = document.createElement("ramBufferSizeMB");
                    ramBufferSizeMBElm.setTextContent(this.ramBufferSizeMB.toString());
                    indexConfig.appendChild(ramBufferSizeMBElm);
                }
                if(this.maxBufferedDocs != null) {
                    final Element maxBufferedDocsElm = document.createElement("maxBufferedDocs");
                    maxBufferedDocsElm.setTextContent(this.maxBufferedDocs.toString());
                    indexConfig.appendChild(maxBufferedDocsElm);
                }
                if(this.ramPerThreadHardLimitMB != null) {
                    final Element ramPerThreadHardLimitMBElm = document.createElement("ramPerThreadHardLimitMB");
                    ramPerThreadHardLimitMBElm.setTextContent(this.ramPerThreadHardLimitMB.toString());
                    indexConfig.appendChild(ramPerThreadHardLimitMBElm);
                }
                if(this.useCompoundFile != null) {
                    final Element useCompoundFileElm = document.createElement("useCompoundFile");
                    useCompoundFileElm.setTextContent(this.useCompoundFile.toString());
                    indexConfig.appendChild(useCompoundFileElm);
                }
                if(this.lockType != null) {
                    final Element lockTypeElm = document.createElement("lockType");
                    lockTypeElm.setTextContent(this.lockType);
                    indexConfig.appendChild(lockTypeElm);
                }
                return indexConfig;
            }
        }

        private static class CodecFactory implements Serializable {

            private String className;
            private String compressionMode;

            public List<String> validate() {
                final List<String> errorMessages = new ArrayList<>();
                return errorMessages;
            }

            public void setDefaults() {
                if(this.className == null) {
                    this.className = "solr.SchemaCodecFactory"; // or solr.SimpleTextCodecFactory
                }
                if(this.compressionMode == null) {
                    this.compressionMode = "BEST_SPEED"; // BEST_SPEED or BEST_COMPRESSION
                }
            }

            public Element createElement(final Document document) {
                final Element codecFactory = document.createElement("codecFactory");
                codecFactory.setAttribute("class", this.className);
                return codecFactory;
            }

        }

        private static class Lib implements Serializable {

            private String dir;
            private String regex;

            public List<String> validate(int index) {
                final List<String> errorMessages = new ArrayList<>();
                if(this.dir == null) {
                    errorMessages.add("Solrindex source module solrconfig.lib[" + index + "].dir must not be null.");
                }
                return errorMessages;
            }

            public void setDefaults() {
                if(this.regex == null) {
                    this.regex = ".*\\.jar";
                }
            }

            public Element createElement(final Document document) {
                final Element lib = document.createElement("lib");
                lib.setAttribute("dir", this.dir);
                lib.setAttribute("regex", this.regex);
                return lib;
            }

        }

        private static class RequestHandler implements Serializable {

            private String name;
            private String className;
            private String startup;
            private Map<String, String> defaults;
            private Map<String, String> invariants;
            private List<String> components;

            public List<String> validate(int index) {
                final List<String> errorMessages = new ArrayList<>();
                if(this.name == null) {
                    errorMessages.add("Solrindex source module solrconfig.requestHandler[" + index + "].name must not be null.");
                }
                if(this.className == null) {
                    errorMessages.add("Solrindex source module solrconfig.requestHandler[" + index + "].className must not be null.");
                }
                return errorMessages;
            }

            public void setDefaults() {
                if(this.defaults == null) {
                    this.defaults = new HashMap<>();
                }
                if(this.invariants == null) {
                    this.invariants = new HashMap<>();
                }
                if(this.components == null) {
                    this.components = new ArrayList<>();
                }
            }

            public Element createElement(final Document document) {
                final Element requestHandler = document.createElement("requestHandler");
                requestHandler.setAttribute("name", this.name);
                requestHandler.setAttribute("class", this.className);
                if(startup != null) {
                    requestHandler.setAttribute("startup", this.startup);
                }
                if(this.defaults.size() > 0) {
                    final Element defaults = document.createElement("lst");
                    defaults.setAttribute("name", "defaults");
                    for(final Map.Entry<String, String> entry : this.defaults.entrySet()) {
                        final Element d = document.createElement("str");
                        d.setAttribute("name", entry.getKey());
                        d.setTextContent(entry.getValue());
                        defaults.appendChild(d);
                    }
                    requestHandler.appendChild(defaults);
                }
                if(this.invariants.size() > 0) {
                    final Element invariants = document.createElement("lst");
                    invariants.setAttribute("name", "invariants");
                    for(final Map.Entry<String, String> entry : this.invariants.entrySet()) {
                        final Element i = document.createElement("str");
                        i.setAttribute("name", entry.getKey());
                        i.setTextContent(entry.getValue());
                        invariants.appendChild(i);
                    }
                    requestHandler.appendChild(invariants);
                }
                if(this.components.size() > 0) {
                    final Element components = document.createElement("arr");
                    components.setAttribute("name", "components");
                    for(final String component : this.components) {
                        final Element c = document.createElement("str");
                        c.setTextContent(component);
                        components.appendChild(c);
                    }
                    requestHandler.appendChild(components);
                }
                return requestHandler;
            }

        }

        private static class SearchComponent implements Serializable {

            private String name;
            private String className;

            public List<String> validate(int index) {
                final List<String> errorMessages = new ArrayList<>();
                if(this.name == null) {
                    errorMessages.add("Solrindex source module solrconfig.searchComponent[" + index + "].name must not be null.");
                }
                if(this.className == null) {
                    errorMessages.add("Solrindex source module solrconfig.searchComponent[" + index + "].className must not be null.");
                }
                return errorMessages;
            }

            public Element createElement(final Document document) {
                final Element searchComponent = document.createElement("searchComponent");
                searchComponent.setAttribute("name", this.name);
                searchComponent.setAttribute("class", this.className);
                return searchComponent;
            }
        }

        private static class UpdateHandler implements Serializable {

            private String className;
            private Map<String, String> updateLog;

            public List<String> validate(int index) {
                final List<String> errorMessages = new ArrayList<>();
                return errorMessages;
            }

            public void setDefaults() {
                if(className == null) {
                    this.className = "solr.DirectUpdateHandler2";
                }
                if(this.updateLog == null) {
                    this.updateLog = new HashMap<>();
                }
                if(!updateLog.containsKey("dir")) {
                    updateLog.put("dir", "${solr.ulog.dir:}");
                }
                if(!updateLog.containsKey("syncLevel")) {
                    updateLog.put("syncLevel", "FSYNC");
                }
                if(!updateLog.containsKey("numRecordsToKeep")) {
                    updateLog.put("numRecordsToKeep", "10");
                }
                if(!updateLog.containsKey("maxNumLogsToKeep")) {
                    updateLog.put("maxNumLogsToKeep", "10");
                }
                if(!updateLog.containsKey("numVersionBuckets")) {
                    updateLog.put("numVersionBuckets", "65536");
                }
            }

            public Element createElement(final Document document) {
                final Element updateHandler = document.createElement("updateHandler");
                updateHandler.setAttribute("class", this.className);
                if(!this.updateLog.isEmpty()) {
                    final Element updateLog = document.createElement("updateLog");
                    for(final Map.Entry<String, String> entry : this.updateLog.entrySet()) {
                        final Element d = switch (entry.getKey()) {
                            case "dir", "syncLevel" -> document.createElement("str");
                            case "numRecordsToKeep", "maxNumLogsToKeep", "numVersionBuckets" -> document.createElement("int");
                            default -> throw new IllegalArgumentException("Nut supported parameter for updateHandler.updateLog." + entry.getKey());
                        };
                        d.setAttribute("name", entry.getKey());
                        d.setTextContent(entry.getValue());

                        updateLog.appendChild(d);
                    }
                    updateHandler.appendChild(updateLog);
                }

                return updateHandler;
            }

        }

        private static class AutoCommit implements Serializable {

            private Long maxDocs;
            private Long maxTime;
            private String maxSize;
            private Boolean openSearcher;

            public List<String> validate() {
                final List<String> errorMessages = new ArrayList<>();
                return errorMessages;
            }

            public void setDefaults() {
                if(this.maxDocs == null) {
                    this.maxDocs = 10000L;
                }
                if(this.maxTime == null) {
                    this.maxTime = 10000L;
                }
                if(this.maxSize == null) {
                    this.maxSize = "";
                }
                if(this.openSearcher == null) {
                    this.openSearcher = false;
                }
            }

            public Element createElement(final Document document) {
                final Element autoCommit = document.createElement("autoCommit");
                {
                    final Element maxDocs = document.createElement("maxDocs");
                    maxDocs.setTextContent(this.maxDocs.toString());
                    autoCommit.appendChild(maxDocs);
                }
                {
                    final Element maxTime = document.createElement("maxTime");
                    maxTime.setTextContent(this.maxTime.toString());
                    autoCommit.appendChild(maxTime);
                }
                {
                    final Element maxSize = document.createElement("maxSize");
                    maxSize.setTextContent(this.maxSize);
                    autoCommit.appendChild(maxSize);
                }

                return autoCommit;
            }

        }

        private static class UpdateLog implements Serializable {

            private String dir;

            public List<String> validate() {
                final List<String> errorMessages = new ArrayList<>();
                return errorMessages;
            }

            public void setDefaults() {
                if(this.dir == null) {
                    this.dir = "${solr.ulog.dir:}"; // or solr.SimpleTextCodecFactory
                }
            }

            public Element createElement(final Document document) {
                final Element updateLog = document.createElement("updateLog");
                final Element dir = document.createElement("str");
                dir.setAttribute("name", "dir");
                dir.setTextContent(this.dir);
                updateLog.appendChild(dir);
                return updateLog;
            }

        }

    }

    public static SolrConfig createSolrConfig() {
        final SolrConfig solrconfig = new SolrConfig();
        solrconfig.setDefaults();
        return solrconfig;
    }

    public static Schema parseIndexSchema(final String indexSchemaXML) {
        try(final InputStream is = new ByteArrayInputStream(indexSchemaXML.getBytes())) {
            final DocumentBuilder documentBuilder = DocumentBuilderFactory
                    .newInstance()
                    .newDocumentBuilder();
            final Document document = documentBuilder.parse(is);

            return parseIndexSchema(document);
        } catch (Exception e) {
            LOG.error("Failed to parse schema.xml: " + indexSchemaXML);
            throw new IllegalStateException("Failed to parse schema.xml: " + indexSchemaXML, e);
        }
    }

    public static Schema parseIndexSchema(final Document indexSchemaDocument) {
        // types
        final NodeList typeNodeList = indexSchemaDocument.getElementsByTagName("types");
        if(typeNodeList.getLength() == 0) {
            throw new IllegalArgumentException();
        }

        final Node types = typeNodeList.item(0);
        final NodeList ts = types.getChildNodes();

        final Map<String, Schema.FieldType> fieldTypes = new HashMap<>();
        final Map<String, Map<String, String>> fieldOptions = new HashMap<>();
        for(int i=0; i<ts.getLength(); i++) {
            final Node node = ts.item(i);
            if(!"fieldType".equals(node.getNodeName())) {
                continue;
            }
            if(node.getAttributes().getNamedItem("name") == null) {
                continue;
            }
            if(node.getAttributes().getNamedItem("class") == null) {
                continue;
            }
            final String typeName = node.getAttributes().getNamedItem("name").getNodeValue();
            final String className = node.getAttributes().getNamedItem("class").getNodeValue();
            final Schema.FieldType fieldType;
            switch (className) {
                case "solr.StrField":
                case "solr.TextField":
                    fieldType = Schema.FieldType.STRING;
                    break;
                case "solr.BoolField":
                    fieldType = Schema.FieldType.BOOLEAN;
                    break;
                case "solr.BinaryField":
                    fieldType = Schema.FieldType.BYTES;
                    break;
                case "solr.IntPointField":
                    fieldType = Schema.FieldType.INT32;
                    break;
                case "solr.LongPointField":
                    fieldType = Schema.FieldType.INT64;
                    break;
                case "solr.FloatPointField":
                    fieldType = Schema.FieldType.FLOAT;
                    break;
                case "solr.DoublePointField":
                    fieldType = Schema.FieldType.DOUBLE;
                    break;
                case "solr.DatePointField":
                    fieldType = Schema.FieldType.DATETIME;
                    break;
                case "solr.DenseVectorField":
                    fieldType = Schema.FieldType.array(Schema.FieldType.FLOAT);
                    break;
                default:
                    fieldType = null;
                    break;
            }
            if(fieldType != null) {
                fieldTypes.put(typeName, fieldType);
            }

            final Map<String, String> attributes = new HashMap<>();
            for(int j=0; j<node.getAttributes().getLength(); j++) {
                final Node attrNode = node.getAttributes().item(j);
                final String attrName = attrNode.getNodeName();
                if("name".equals(attrName) || "class".equals(attrName)) {
                    continue;
                }
                attributes.put(attrName, attrNode.getNodeValue());
            }
            fieldOptions.put(typeName, attributes);
        }


        final Schema.Builder builder = Schema.builder();

        // fields
        final NodeList nodeList = indexSchemaDocument.getElementsByTagName("fields");
        if(nodeList.getLength() == 0) {
            throw new IllegalArgumentException();
        }

        final Node fields = nodeList.item(0);
        final NodeList fs = fields.getChildNodes();

        for(int i=0; i<fs.getLength(); i++) {
            final Node node = fs.item(i);
            if(!"field".equals(node.getNodeName())) {
                continue;
            }
            if(node.getAttributes().getNamedItem("name") == null) {
                continue;
            }
            if(node.getAttributes().getNamedItem("type") == null) {
                continue;
            }

            final String fieldName = node.getAttributes().getNamedItem("name").getNodeValue();
            final String fieldTypeName = node.getAttributes().getNamedItem("type").getNodeValue();
            final Map<String, String> fieldAttributes = new HashMap<>();
            for(int j=0; j<node.getAttributes().getLength(); j++) {
                final Node attrNode = node.getAttributes().item(j);
                final String attrName = attrNode.getNodeName();
                if("name".equals(attrName) || "type".equals(attrName)) {
                    continue;
                }
                fieldAttributes.put(attrName, attrNode.getNodeValue());
            }
            if(fieldTypes.containsKey(fieldTypeName)) {
                final Schema.FieldType fieldType = fieldTypes.get(fieldTypeName);
                final Boolean stored = Boolean.valueOf(fieldAttributes.getOrDefault("stored", "false"));
                if(!stored) {
                    continue;
                }
                final Boolean required = Boolean.valueOf(fieldAttributes.getOrDefault("required", "false"));
                final Boolean multiValued = Boolean.valueOf(fieldAttributes.getOrDefault("multiValued", "false"));

                if(multiValued) {
                    builder.addField(fieldName, Schema.FieldType.array(fieldType).withNullable(!required));
                } else {
                    builder.addField(fieldName, fieldType.withNullable(!required));
                }
            }

            /*
            if (node.hasChildNodes()) {
                final NodeList cfs = node.getChildNodes();
                for (int j=0; j<cfs.getLength(); j++) {
                    final Node childNode = cfs.item(j);
                    if(!"field".equals(childNode.getNodeName())) {
                        continue;
                    }
                    if(childNode.getAttributes().getNamedItem("name") == null) {
                        continue;
                    }
                    if(childNode.getAttributes().getNamedItem("type") == null) {
                        continue;
                    }
                    final String childFieldName = childNode.getAttributes().getNamedItem("name").getNodeValue();
                    final String childFieldType = childNode.getAttributes().getNamedItem("name").getNodeValue();
                }
            }
             */
        }

        final Schema.Options.Builder optionBuilder = Schema.Options.builder()
                .setOption("name", Schema.FieldType.STRING, "searchResults");

        builder.setOptions(optionBuilder);
        return builder.build();
    }

    public static Document convertIndexSchemaJsonToXml(final JsonObject jsonObject) {
        final Document document = createSchemaXML("content");
        final Element root = document.getDocumentElement();

        // fields
        if(jsonObject.has("fields") && jsonObject.get("fields").isJsonArray()) {
            final Element fields = document.createElement("fields");
            for(final JsonElement field : jsonObject.getAsJsonArray("fields")) {
                final JsonObject value = field.getAsJsonObject();

                final Element fieldElement;
                if(value.has("tag")) {
                    fieldElement = document.createElement(value.getAsJsonPrimitive("tag").getAsString());
                } else {
                    fieldElement = document.createElement("field");
                }
                for(final Map.Entry<String, JsonElement> entry : value.entrySet()) {
                    if("tag".equals(entry.getKey())) {
                        continue;
                    }
                    fieldElement.setAttribute(entry.getKey(), entry.getValue().getAsString());
                }
            }
            root.appendChild(fields);
        }

        // uniqueKey
        if(jsonObject.has("uniqueKey") && jsonObject.get("uniqueKey").isJsonPrimitive()) {
            final Element uniqueKey = document.createElement("uniqueKey");
            uniqueKey.setTextContent(jsonObject.get("uniqueKey").getAsString());
            root.appendChild(uniqueKey);
        }

        // types
        if(jsonObject.has("types") && jsonObject.get("types").isJsonArray()) {
            final Element types = document.createElement("types");
            for(final JsonElement field : jsonObject.getAsJsonArray("types")) {
                final JsonObject value = field.getAsJsonObject();

                final Element typeElement;
                if(value.has("tag")) {
                    typeElement = document.createElement(value.getAsJsonPrimitive("tag").getAsString());
                } else {
                    typeElement = document.createElement("fieldType");
                }
                for(final Map.Entry<String, JsonElement> entry : value.entrySet()) {
                    if("tag".equals(entry.getKey())) {
                        continue;
                    }
                    if(entry.getValue().isJsonPrimitive()) {
                        typeElement.setAttribute(entry.getKey(), entry.getValue().getAsString());
                    } else if(entry.getValue().isJsonObject()) {
                        final JsonObject child = entry.getValue().getAsJsonObject();
                    }
                }
            }
            root.appendChild(types);
        }

        return document;
    }

    public static Document createSchemaXML(final String coreName) {
        final Document document = XmlUtil.createDocument("schema");
        ((Element)document.getFirstChild()).setAttribute("name", coreName);
        ((Element)document.getFirstChild()).setAttribute("version", "1.6");
        return document;
    }

    public static void setDefaultSchemaTypes(final Document document, final Element types) {

        // string
        final Element stringType = document.createElement("fieldType");
        stringType.setAttribute("name", "string");
        stringType.setAttribute("class", "solr.StrField");
        stringType.setAttribute("sortMissingLast", "true");
        types.appendChild(stringType);
        // boolean
        final Element booleanType = document.createElement("fieldType");
        booleanType.setAttribute("name", "boolean");
        booleanType.setAttribute("class", "solr.BoolField");
        booleanType.setAttribute("sortMissingLast", "true");
        types.appendChild(booleanType);
        // bytes
        final Element bytesType = document.createElement("fieldType");
        bytesType.setAttribute("name", "string");
        bytesType.setAttribute("class", "solr.BinaryField");
        bytesType.setAttribute("sortMissingLast", "true");
        types.appendChild(bytesType);
        // int
        final Element intType = document.createElement("fieldType");
        intType.setAttribute("name", "int");
        intType.setAttribute("class", "solr.IntPointField");
        intType.setAttribute("sortMissingLast", "true");
        types.appendChild(intType);
        // long
        final Element longType = document.createElement("fieldType");
        longType.setAttribute("name", "long");
        longType.setAttribute("class", "solr.LongPointField");
        longType.setAttribute("sortMissingLast", "true");
        types.appendChild(longType);
        // float
        final Element floatType = document.createElement("fieldType");
        floatType.setAttribute("name", "float");
        floatType.setAttribute("class", "solr.FloatPointField");
        floatType.setAttribute("sortMissingLast", "true");
        types.appendChild(floatType);
        // double
        final Element doubleType = document.createElement("fieldType");
        doubleType.setAttribute("name", "double");
        doubleType.setAttribute("class", "solr.DoublePointField");
        doubleType.setAttribute("sortMissingLast", "true");
        types.appendChild(doubleType);
        // date
        final Element dateType = document.createElement("fieldType");
        dateType.setAttribute("name", "date");
        dateType.setAttribute("class", "solr.DatePointField");
        dateType.setAttribute("sortMissingLast", "true");
        types.appendChild(dateType);
        // vector
        final Element vectorType = document.createElement("fieldType");
        vectorType.setAttribute("name", "vector");
        vectorType.setAttribute("class", "solr.DenseVectorField");
        vectorType.setAttribute("vectorDimension", "4");
        vectorType.setAttribute("similarityFunction", "cosine");
        types.appendChild(vectorType);

        // text_ja
        final Element textJaType = document.createElement("fieldType");
        textJaType.setAttribute("name", "textja");
        textJaType.setAttribute("class", "solr.TextField");
        textJaType.setAttribute("positionIncrementGap", "100");
        {
            final Element analyzer = document.createElement("analyzer");
            {
                final Element tokenizer = document.createElement("tokenizer");
                tokenizer.setAttribute("class", "solr.JapaneseTokenizerFactory");
                tokenizer.setAttribute("mode", "search");
                analyzer.appendChild(tokenizer);
            }
            {
                final Element filter = document.createElement("filter");
                filter.setAttribute("class", "solr.JapaneseBaseFormFilterFactory");
                analyzer.appendChild(filter);
            }
            {
                final Element filter = document.createElement("filter");
                filter.setAttribute("class", "solr.CJKWidthFilterFactory");
                analyzer.appendChild(filter);
            }
            {
                final Element filter = document.createElement("filter");
                filter.setAttribute("class", "solr.LowerCaseFilterFactory");
                analyzer.appendChild(filter);
            }
            {
                final Element filter = document.createElement("filter");
                filter.setAttribute("class", "solr.JapaneseKatakanaStemFilterFactory");
                filter.setAttribute("minimumLength", "2");
                analyzer.appendChild(filter);
            }
            textJaType.appendChild(analyzer);
        }
        types.appendChild(textJaType);

        // text_bi
        final Element textBiType = document.createElement("fieldType");
        textBiType.setAttribute("name", "textbi");
        textBiType.setAttribute("class", "solr.TextField");
        textBiType.setAttribute("positionIncrementGap", "100");
        {
            final Element analyzer = document.createElement("analyzer");
            {
                final Element tokenizer = document.createElement("tokenizer");
                tokenizer.setAttribute("class", "solr.NGramTokenizerFactory");
                tokenizer.setAttribute("minGramSize", "2");
                tokenizer.setAttribute("maxGramSize", "2");
                analyzer.appendChild(tokenizer);
            }
            {
                final Element filter = document.createElement("filter");
                filter.setAttribute("class", "solr.LowerCaseFilterFactory");
                analyzer.appendChild(filter);
            }
            textBiType.appendChild(analyzer);
        }
        types.appendChild(textBiType);


        // random
        final Element randomType = document.createElement("fieldType");
        randomType.setAttribute("name", "random");
        randomType.setAttribute("class", "solr.RandomSortField");
        types.appendChild(randomType);

        // ignore
        final Element ignoreType = document.createElement("fieldType");
        ignoreType.setAttribute("name", "ignored");
        ignoreType.setAttribute("indexed", "false");
        ignoreType.setAttribute("stored", "false");
        ignoreType.setAttribute("class", "solr.StrField");
        types.appendChild(ignoreType);

    }

    public static List<String> getFieldNames(final String schemaString) {

        try(final InputStream is = new ByteArrayInputStream(schemaString.getBytes())) {
            final DocumentBuilder documentBuilder = DocumentBuilderFactory
                    .newInstance()
                    .newDocumentBuilder();
            final Document document = documentBuilder.parse(is);

            final NodeList nodeList = document.getElementsByTagName("fields");
            if(nodeList.getLength() == 0) {
                return new ArrayList<>();
            }

            final Node fields = nodeList.item(0);
            final NodeList fs = fields.getChildNodes();

            final List<String> fieldNames = new ArrayList<>();
            for(int i=0; i<fs.getLength(); i++) {
                final Node node = fs.item(i);
                if(!"field".equals(node.getNodeName())) {
                    continue;
                }
                if(node.getAttributes().getNamedItem("name") == null) {
                    continue;
                }
                fieldNames.add(node.getAttributes().getNamedItem("name").getNodeValue());

                if (node.hasChildNodes()) {
                    final NodeList cfs = node.getChildNodes();
                    for (int j=0; j<cfs.getLength(); j++) {
                        final Node childNode = cfs.item(j);
                        if(!"field".equals(childNode.getNodeName())) {
                            continue;
                        }
                        if(childNode.getAttributes().getNamedItem("name") == null) {
                            continue;
                        }
                        fieldNames.add(childNode.getAttributes().getNamedItem("name").getNodeValue());
                    }
                }
            }
            return fieldNames;
        } catch (Exception e) {
            LOG.error("Failed to parse schema.xml: " + schemaString);
            throw new IllegalStateException("Failed to parse schema.xml: " + schemaString, e);
        }
    }

}
