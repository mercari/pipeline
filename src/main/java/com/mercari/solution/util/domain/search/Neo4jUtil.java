package com.mercari.solution.util.domain.search;

/*
import com.google.common.reflect.ClassPath;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.gcp.StorageUtil;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.joda.time.Instant;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.archive.*;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.Predicate;
import java.util.stream.Collectors;
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;


public class Neo4jUtil implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Neo4jUtil.class);

    public static final String DEFAULT_DATABASE_NAME = "neo4j";

    public static class NodeConfig implements Serializable {

        private String input;
        private List<String> labels;
        private List<String> keyFields;
        private List<String> propertyFields;

        public String getInput() {
            return input;
        }

        public List<String> getLabels() {
            return labels;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public List<String> getPropertyFields() {
            return propertyFields;
        }


        public List<String> validate(int index) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.input == null) {
                errorMessages.add("localNeo4j.nodes[" + index + "].input parameter must not be null.");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(labels == null) {
                labels = new ArrayList<>();
            }
            if(keyFields == null) {
                keyFields = new ArrayList<>();
            }
            if(propertyFields == null) {
                propertyFields = new ArrayList<>();
            }
        }
    }

    public static class RelationshipConfig implements Serializable {

        private String input;
        private String type;
        private List<String> keyFields;
        private List<String> propertyFields;
        private RelationshipNodeConfig source;
        private RelationshipNodeConfig target;

        public String getInput() {
            return input;
        }

        public String getType() {
            return type;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public List<String> getPropertyFields() {
            return propertyFields;
        }

        public RelationshipNodeConfig getSource() {
            return source;
        }

        public RelationshipNodeConfig getTarget() {
            return target;
        }

        public List<String> validate(int index) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.input == null) {
                errorMessages.add("localNeo4j.relationships[" + index + "].input parameter must not be null.");
            }
            if(this.type == null) {
                errorMessages.add("localNeo4j.relationships[" + index + "].type parameter must not be null.");
            }
            if(this.source == null) {
                errorMessages.add("localNeo4j.relationships[" + index + "].source parameter must not be null.");
            } else {
                errorMessages.addAll(this.source.validate("source", index));
            }
            if(this.target == null) {
                errorMessages.add("localNeo4j.relationships[" + index + "].target parameter must not be null.");
            } else {
                errorMessages.addAll(this.target.validate("target", index));
            }

            return errorMessages;
        }

        public void setDefaults() {
            if(keyFields == null) {
                keyFields = new ArrayList<>();
            }
            if(propertyFields == null) {
                propertyFields = new ArrayList<>();
            }
            source.setDefaults();
            target.setDefaults();
        }
    }

    public static class RelationshipNodeConfig implements Serializable {

        private String label;
        private List<String> keyFields;
        private List<String> propertyFields;

        public String getLabel() {
            return label;
        }

        public List<String> getLabels() {
            return Arrays.asList(label);
        }
        public List<String> getKeyFields() {
            return keyFields;
        }

        public List<String> getPropertyFields() {
            return propertyFields;
        }

        public List<String> validate(final String type, int index) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.label == null) {
                errorMessages.add("localNeo4j.relationships[" + index + "]." + type + ".label parameter must not be null.");
            }
            if(this.keyFields == null || keyFields.size() == 0) {
                errorMessages.add("localNeo4j.relationships[" + index + "]." + type + ".keyFields parameter must not be null and must not be size zero.");
            }

            return errorMessages;
        }

        public void setDefaults() {
            if(keyFields == null) {
                keyFields = new ArrayList<>();
            }
            if(propertyFields == null) {
                propertyFields = new ArrayList<>();
            }
        }

    }

    public enum Format {
        dump,
        zip
    }

    /*
    public static void query(final HttpClient client, final String username, final String password, final String endpoint, final String database, final String cypher) {
        final String url = String.format("%s/db/%s/tx/commit", endpoint, database);

        final JsonArray statements = new JsonArray();
        final JsonObject statement = new JsonObject();
        statement.addProperty("statement", cypher);
        statements.add(statement);

        final JsonObject body = new JsonObject();
        body.add("statements", statements);

        try {
            final HttpRequest req = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .header("content-type", "application/json")
                    .header("Authorization", "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8)))
                    .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                    .build();

            final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            final JsonObject responseJson = new Gson().fromJson(res.body(), JsonObject.class);
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new RuntimeException("Failed to getEndpointID: " + url, e);
        }
    }

    public static void index(final GraphDatabaseService graphDB,
                             final List<MElement> buffer,
                             final List<NodeConfig> nodes,
                             final List<RelationshipConfig> relationships,
                             final List<String> inputNames) {

        if(buffer == null || buffer.size() == 0) {
            LOG.info("no update index");
            return;
        }

        int countNode = 0;
        try(final Transaction tx = graphDB.beginTx()) {
            for(final MElement element : buffer) {
                final int index = element.getIndex();
                if(inputNames.size() <= index) {
                    throw new IllegalStateException("UnionValue index: " + index + " is over inputs size: " + inputNames.size());
                }
                final String sourceInputName = inputNames.get(index);

                for(final Neo4jUtil.NodeConfig nodeConfig : nodes) {
                    if(!nodeConfig.getInput().equals(sourceInputName)) {
                        continue;
                    }

                    final Node node = getNode(tx, nodeConfig.getLabels(), nodeConfig.getKeyFields(), element);
                    final Map<String, Object> properties = element.asPrimitiveMap(nodeConfig.getPropertyFields());
                    for(final Map.Entry<String, Object> property : properties.entrySet()) {
                        final Object propertyValue = formatValue(property.getValue());
                        node.setProperty(property.getKey(), propertyValue);
                    }
                    countNode += 1;
                }
            }
            tx.commit();
        }

        // update relationships
        int countRelationship = 0;
        try(final Transaction tx = graphDB.beginTx()) {
            for(final MElement element : buffer) {
                final int index = element.getIndex();
                if(inputNames.size() <= index) {
                    throw new IllegalStateException("UnionValue index: " + index + " is over inputs size: " + inputNames.size());
                }
                final String sourceInputName = inputNames.get(index);

                for(final Neo4jUtil.RelationshipConfig relationshipConfig : relationships) {
                    if(!relationshipConfig.getInput().equals(sourceInputName)) {
                        continue;
                    }

                    final RelationshipNodeConfig sourceConfig = relationshipConfig.getSource();
                    final Node source = getNode(tx, sourceConfig.getLabels(), sourceConfig.getKeyFields(), element);
                    if(!sourceConfig.getPropertyFields().isEmpty()) {
                        Map<String, Object> properties = element.asPrimitiveMap(sourceConfig.getPropertyFields());
                        for(final Map.Entry<String, Object> property : properties.entrySet()) {
                            final Object propertyValue = formatValue(property.getValue());
                            source.setProperty(property.getKey(), propertyValue);
                        }
                    }

                    final RelationshipNodeConfig targetConfig = relationshipConfig.getTarget();
                    final Node target = getNode(tx, targetConfig.getLabels(), targetConfig.getKeyFields(), element);
                    if(targetConfig.getPropertyFields().size() > 0) {
                        Map<String, Object> properties = element.asPrimitiveMap(targetConfig.getPropertyFields());
                        for(final Map.Entry<String, Object> property : properties.entrySet()) {
                            final Object propertyValue = formatValue(property.getValue());
                            target.setProperty(property.getKey(), propertyValue);
                        }
                    }

                    final Relationship relationship = getRelationship(tx, source, target,
                            relationshipConfig.getType(), relationshipConfig.getKeyFields(), element);
                    final Map<String, Object> properties = element.asPrimitiveMap(relationshipConfig.getPropertyFields());
                    for(final Map.Entry<String, Object> property : properties.entrySet()) {
                        final Object propertyValue = formatValue(property.getValue());
                        relationship.setProperty(property.getKey(), propertyValue);
                    }
                    countRelationship += 1;
                }
            }
            tx.commit();
        }
        LOG.info("update node: " + countNode + ", relationship: " + countRelationship);
    }

    public static void dump(final String neo4jHome, final String databaseName, final OutputStream os) throws IOException {
        final DatabaseLayout layout = Neo4jLayout.of(Path.of(neo4jHome)).databaseLayout(databaseName);
        final CompressionFormat format = DumpFormatSelector.selectFormat();
        final String lockFile = layout.databaseLockFile().getFileName().toString();
        final String quarantineMarkerFile = layout.quarantineFile().getFileName().toString();
        LOG.info("dump databasePath: " + layout.databaseDirectory().toAbsolutePath()
                + ", transactionLogsPath: " + layout.getTransactionLogsDirectory().toAbsolutePath()
                + ", compressionFormat: " + format.getClass().getName());

        dump(layout.databaseDirectory(), layout.getTransactionLogsDirectory(), os, format, path -> oneOf(path, lockFile, quarantineMarkerFile));
    }

    public static void dump(final Path dbPath, final Path transactionLogPath, final OutputStream os, final CompressionFormat format, final Predicate<Path> exclude) throws IOException {
        final Dumper dumper = new Dumper(new DefaultFileSystemAbstraction());
        dumper.dump(dbPath, transactionLogPath, os, format, exclude);
    }

    public static void load(final String neo4jHome, final String databaseName, final String archivePath) throws IOException {
        final DatabaseLayout layout = Neo4jLayout.of(Path.of(neo4jHome)).databaseLayout(databaseName);
        final Path databaseDestination = layout.databaseDirectory();
        final Path transactionLogsDirectory = layout.getTransactionLogsDirectory();
        LOG.info("load databasePath: " + layout.databaseDirectory().toAbsolutePath()
                + ", transactionLogsPath: " + layout.getTransactionLogsDirectory().toAbsolutePath()
                + ", from archivePath: " + archivePath);

        load(databaseDestination, transactionLogsDirectory, archivePath);
    }

    public static void load(final Path dbPath, final Path transactionLogPath, final String archivePath) throws IOException {
        final FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        final DecompressionSelector selector = DumpFormatSelector::decompress;
        final ThrowingSupplier<InputStream, IOException> streamSupplier = () -> StorageUtil.readStream(StorageUtil.storage(), archivePath);
        try (ArchiveInputStream stream = openArchiveIn(selector, streamSupplier, archivePath)) {
            ArchiveEntry entry;
            while ((entry = stream.getNextEntry()) != null) {
                Path destination = determineEntryDestination(entry, dbPath, transactionLogPath);
                loadEntry(destination, stream, entry, fileSystem);
            }
        }
    }

    public static void registerShutdownHook(final DatabaseManagementService managementService) {
        Runtime.getRuntime().addShutdownHook(new Thread(managementService::shutdown));
    }

    public static void setupGds(final GraphDatabaseService graphDb) {
        final GlobalProcedures registry = ((GraphDatabaseAPI) graphDb)
                .getDependencyResolver()
                .resolveDependency(GlobalProcedures.class, DependencyResolver.SelectionStrategy.SINGLE);
        try {
            final ClassLoader loader = Thread.currentThread().getContextClassLoader();

            final Set<Class<?>> procedureClasses = new HashSet<>();
            procedureClasses.addAll(getClasses(loader, "org.neo4j.gds.graphalgo", "Proc"));
            procedureClasses.addAll(getClasses(loader, "org.neo4j.gds.embeddings", "Proc"));
            procedureClasses.addAll(getClasses(loader, "org.neo4j.gds.paths", "Proc"));
            procedureClasses.addAll(getClasses(loader, "org.neo4j.gds.catalog", "Proc"));

            for (final Class<?> procedureClass : procedureClasses) {
                LOG.info("registerProcedure class: " + procedureClass.getName());
                registry.registerProcedure(procedureClass);
            }

            final Set<Class<?>> functionClasses = new HashSet<>();
            functionClasses.addAll(getClasses(loader, "org.neo4j.gds.functions", "Func"));
            for (final Class<?> functionClass : functionClasses) {
                LOG.info("registerFunction class: " + functionClass.getName());
                registry.registerFunction(functionClass);
            }

        } catch (ProcedureException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Set<Class<?>> getClasses(final ClassLoader loader, final String packageName, final String suffix) throws IOException {
        return ClassPath
                .from(loader)
                .getTopLevelClassesRecursive(packageName)
                .stream()
                .filter(f -> f.getSimpleName().endsWith(suffix))
                .map(ClassPath.ClassInfo::load)
                .collect(Collectors.toSet());
    }

    private static Node getNode(final Transaction tx, final List<String> labelNames, final List<String> keyFields, final MElement element) {
        final Label[] labels = labelNames
                .stream()
                .map(Label::label)
                .toArray(Label[]::new);
        final Map<String, Object> keyProperties = element.asPrimitiveMap(keyFields);
        try(final ResourceIterator<Node> nodes = tx.findNodes(labels[0], keyProperties)) {
            if(nodes.hasNext()) {
                return nodes.next();
            } else {
                final Node node = tx.createNode(labels);
                for(final Map.Entry<String, Object> property : keyProperties.entrySet()) {
                    final Object propertyValue = formatValue(property.getValue());
                    node.setProperty(property.getKey(), propertyValue);
                }
                return node;
            }
        }
    }

    private static Relationship getRelationship(final Transaction tx, final Node source, final Node target, final String type, final List<String> keyFields, final MElement element) {
        final RelationshipType relationshipType = RelationshipType.withName(type);
        final Map<String, Object> keyProperties;
        if(keyFields != null && !keyFields.isEmpty()) {
            keyProperties = element.asPrimitiveMap(keyFields);
            try(final ResourceIterator<Relationship> relationships = tx.findRelationships(relationshipType, keyProperties)) {
                if(relationships.hasNext()) {
                    return relationships.next();
                }
            }
        } else {
            keyProperties = new HashMap<>();
        }

        final Relationship relationship = source.createRelationshipTo(target, relationshipType);
        for(final Map.Entry<String, Object> property : keyProperties.entrySet()) {
            final Object propertyValue = formatValue(property.getValue());
            relationship.setProperty(property.getKey(), propertyValue);
        }
        return relationship;
    }

    private static Object formatValue(Object value) {
        if(value == null) {
            return null;
        }
        if(value instanceof java.time.Instant) {
            final java.time.Instant instant = (java.time.Instant) value;
            return instant.atZone(ZoneId.of("Etc/GMT"));
        } else if(value instanceof Instant) {
            final Instant instant = (Instant) value;
            return ZonedDateTime.parse(instant.toString());
        } else if(value instanceof Float) {
            return ((Float) value).doubleValue();
        } else if(value instanceof Integer) {
            return ((Integer) value).longValue();
        }
        return value;
    }

    private static boolean oneOf(Path path, String... names) {
        return ArrayUtil.contains(names, path.getFileName().toString());
    }


    private static Path determineEntryDestination(
            ArchiveEntry entry, Path databaseDestination, Path transactionLogsDirectory) {
        Path entryName = Path.of(entry.getName()).getFileName();
        try {
            return TransactionLogFiles.DEFAULT_FILENAME_FILTER.accept(entryName)
                    ? transactionLogsDirectory
                    : databaseDestination;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void loadEntry(Path destination, ArchiveInputStream stream, ArchiveEntry entry, final FileSystemAbstraction filesystem) throws IOException {
        Path file = destination.resolve(entry.getName().replace('\\', '/'));
        var normalizedFile = file.normalize();
        if (!normalizedFile.startsWith(destination)) {
            throw new InvalidDumpEntryException(entry.getName());
        }

        if (entry.isDirectory()) {
            filesystem.mkdirs(normalizedFile);
        } else {
            filesystem.mkdirs(normalizedFile.getParent());
            try (OutputStream output = filesystem.openAsOutputStream(normalizedFile, false)) {
                copy(stream, output);
            }
        }
    }

    private static ArchiveInputStream openArchiveIn(
            DecompressionSelector selector, ThrowingSupplier<InputStream, IOException> streamSupplier, String name)
            throws IOException {
        try {
            InputStream decompressor = selector.decompress(streamSupplier);

            if (StandardCompressionFormat.ZSTD.isFormat(decompressor)) {
                // Important: Only the ZSTD compressed archives have any archive metadata.
                readArchiveMetadata(decompressor);
            }

            return new TarArchiveInputStream(decompressor);
        } catch (NoSuchFileException ioe) {
            throw ioe;
        } catch (IOException e) {
            throw new RuntimeException("Failed to open archive: "+ name, e);
        }
    }

    private static void readArchiveMetadata(InputStream stream) throws IOException {
        final DataInputStream metadata = new DataInputStream(stream); // Unbuffered. Will not play naughty tricks with the file position.
        int version = metadata.readInt();
        if (version == 1) {
            long maxFiles = metadata.readLong();
            long maxBytes = metadata.readLong();
        } else {
            throw new IOException("Cannot read archive meta-data. I don't recognise this archive version: " + version + ".");
        }
    }

    public static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[8192];
        int n;
        while(-1 != (n = in.read(buffer))) {
            out.write(buffer, 0, n);
        }
    }

     */

}
