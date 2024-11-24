package com.mercari.solution.module.transform;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.protobuf.ByteString;
import com.mercari.solution.config.TransformConfig;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.FCollection;
import com.mercari.solution.util.*;
import com.mercari.solution.util.domain.text.JsonUtil;
import com.mercari.solution.util.gcp.StorageUtil;
import com.mercari.solution.util.hashicorp.VaultClient;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.EntitySchemaUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.StructSchemaUtil;
import com.mercari.solution.util.schema.converter.JsonToMapConverter;
import freemarker.template.Template;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

public class CryptoTransform {


    private static final Logger LOG = LoggerFactory.getLogger(CryptoTransform.class);
    private static final String OUTPUT_SUFFIX_FAILURES = ".failures";

    private class CryptoTransformParameters implements Serializable {

        private String mode; // decrypt or encrypt
        private List<String> fields;
        private String algorithm;
        private Boolean failFast;

        private KeyProviderParameter keyProvider;
        private KeyDecryptorParameter keyDecryptor;
        private KeyExtractorParameter keyExtractor;

        private VaultParameter vault;

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }

        public List<String> getFields() {
            return fields;
        }

        public void setFields(List<String> fields) {
            this.fields = fields;
        }

        public String getAlgorithm() {
            return algorithm;
        }

        public void setAlgorithm(String algorithm) {
            this.algorithm = algorithm;
        }

        public Boolean getFailFast() {
            return failFast;
        }

        public void setFailFast(Boolean failFast) {
            this.failFast = failFast;
        }

        public KeyProviderParameter getKeyProvider() {
            return keyProvider;
        }

        public void setKeyProvider(KeyProviderParameter keyProvider) {
            this.keyProvider = keyProvider;
        }

        public KeyDecryptorParameter getKeyDecryptor() {
            return keyDecryptor;
        }

        public void setKeyDecryptor(KeyDecryptorParameter keyDecryptor) {
            this.keyDecryptor = keyDecryptor;
        }

        public KeyExtractorParameter getKeyExtractor() {
            return keyExtractor;
        }

        public void setKeyExtractor(KeyExtractorParameter keyExtractor) {
            this.keyExtractor = keyExtractor;
        }

        public VaultParameter getVault() {
            return vault;
        }

        public void setVault(VaultParameter vault) {
            this.vault = vault;
        }

        private class KeyProviderParameter implements Serializable {

            private String base64text;
            private VaultParameter vault;

            public String getBase64text() {
                return base64text;
            }

            public void setBase64text(String base64text) {
                this.base64text = base64text;
            }

            public VaultParameter getVault() {
                return vault;
            }

            public void setVault(VaultParameter vault) {
                this.vault = vault;
            }

            private class VaultParameter implements Serializable {

                private String kvPath;

                public String getKvPath() {
                    return kvPath;
                }

                public void setKvPath(String kvPath) {
                    this.kvPath = kvPath;
                }
            }

        }

        private class KeyDecryptorParameter implements Serializable {

            private String jsonPath;

            private VaultParameter vault;

            public String getJsonPath() {
                return jsonPath;
            }

            public void setJsonPath(String jsonPath) {
                this.jsonPath = jsonPath;
            }

            public VaultParameter getVault() {
                return vault;
            }

            public void setVault(VaultParameter vault) {
                this.vault = vault;
            }

            private class VaultParameter implements Serializable {

                private String transitPath;

                public String getTransitPath() {
                    return transitPath;
                }

                public void setTransitPath(String transitPath) {
                    this.transitPath = transitPath;
                }
            }

        }

        private class KeyExtractorParameter implements Serializable {

            private String template;
            private String jsonPath;

            public String getTemplate() {
                return template;
            }

            public void setTemplate(String template) {
                this.template = template;
            }

            public String getJsonPath() {
                return jsonPath;
            }

            public void setJsonPath(String jsonPath) {
                this.jsonPath = jsonPath;
            }
        }

        private class VaultParameter implements Serializable {

            private String host;
            private String namespace;
            private String role;
            private String tokenPath;

            public String getHost() {
                return host;
            }

            public void setHost(String host) {
                this.host = host;
            }

            public String getNamespace() {
                return namespace;
            }

            public void setNamespace(String namespace) {
                this.namespace = namespace;
            }

            public String getRole() {
                return role;
            }

            public void setRole(String role) {
                this.role = role;
            }

            public String getTokenPath() {
                return tokenPath;
            }

            public void setTokenPath(String tokenPath) {
                this.tokenPath = tokenPath;
            }
        }

    }

    public String getName() { return "crypto"; }

    public Map<String, FCollection<?>> expand(List<FCollection<?>> inputs, TransformConfig config) {
        return CryptoTransform.transform(inputs, config);
    }

    public static Map<String, FCollection<?>> transform(
            final List<FCollection<?>> inputs,
            final TransformConfig config) {

        final CryptoTransformParameters parameters = new Gson().fromJson(config.getParameters(), CryptoTransformParameters.class);
        validateParameters(parameters);
        setDefaultParameters(parameters);

        final Map<String, FCollection<?>> results = new HashMap<>();
        for(final FCollection<?> input : inputs) {
            final String name = config.getName() + (config.getInputs().size() == 1 ? "" : "." + input.getName());
            switch (input.getDataType()) {
                case AVRO: {
                    final FCollection<GenericRecord> inputCollection = (FCollection<GenericRecord>) input;
                    if("decrypt".equals(parameters.getMode().trim().toLowerCase())) {
                        final Decrypt<GenericRecord> transform = new Decrypt<>(
                                parameters,
                                AvroSchemaUtil::getBytes,
                                (GenericRecord r, Map<String, byte[]> decryptedBytes) -> {
                                    final GenericRecordBuilder builder = AvroSchemaUtil.copy(r, r.getSchema());
                                    for (var entry : decryptedBytes.entrySet()) {
                                        if(entry.getValue() == null) {
                                            builder.set(entry.getKey(), null);
                                        } else {
                                            builder.set(entry.getKey(), ByteBuffer.wrap(entry.getValue()));
                                        }
                                    }
                                    return builder.build();
                                });
                        final PCollectionTuple outputs = inputCollection.getCollection().apply(name, transform);
                        final PCollection<?> output = outputs.get(transform.outputTag);
                        final PCollection<?> failures = outputs.get(transform.failuresTag).setCoder(inputCollection.getCollection().getCoder());
                        results.put(name, FCollection.of(config.getName(), output, DataType.AVRO, inputCollection.getAvroSchema()));
                        results.put(name + OUTPUT_SUFFIX_FAILURES, FCollection.of(config.getName(), failures, DataType.AVRO, inputCollection.getAvroSchema()));
                    }
                    break;
                }
                case ROW: {
                    final FCollection<Row> inputCollection = (FCollection<Row>) input;
                    if("decrypt".equals(parameters.getMode().trim().toLowerCase())) {
                        final Decrypt<Row> transform = new Decrypt<>(
                                parameters,
                                RowSchemaUtil::getBytes,
                                (Row row, Map<String, byte[]> decryptedBytes) -> {
                                    final Row.FieldValueBuilder builder = Row.fromRow(row);
                                    for (var entry : decryptedBytes.entrySet()) {
                                        builder.withFieldValue(entry.getKey(), entry.getValue());
                                    }
                                    return builder.build();
                                });
                        final PCollectionTuple outputs = inputCollection.getCollection().apply(name, transform);
                        final PCollection<?> output = outputs.get(transform.outputTag);
                        final PCollection<?> failures = outputs.get(transform.failuresTag).setCoder(inputCollection.getCollection().getCoder());
                        results.put(name, FCollection.of(config.getName(), output, DataType.ROW, inputCollection.getSchema()));
                        results.put(name + OUTPUT_SUFFIX_FAILURES, FCollection.of(config.getName(), failures, DataType.ROW, inputCollection.getSchema()));
                    }
                    break;
                }
                case STRUCT: {
                    final FCollection<Struct> inputCollection = (FCollection<Struct>) input;
                    if("decrypt".equals(parameters.getMode().trim().toLowerCase())) {
                        final Decrypt<Struct> transform = new Decrypt<>(
                                parameters,
                                StructSchemaUtil::getBytes,
                                (Struct struct, Map<String, byte[]> decryptedBytes) -> {
                                    final Struct.Builder builder = StructSchemaUtil.toBuilder(struct, null, decryptedBytes.keySet());
                                    for (var entry : decryptedBytes.entrySet()) {
                                        if(entry.getValue() == null) {
                                            builder.set(entry.getKey()).to((ByteArray) null);
                                        } else {
                                            builder.set(entry.getKey()).to(ByteArray.copyFrom(entry.getValue()));
                                        }
                                    }
                                    return builder.build();
                                });
                        final PCollectionTuple outputs = inputCollection.getCollection().apply(name, transform);
                        final PCollection<?> output = outputs.get(transform.outputTag);
                        final PCollection<?> failures = outputs.get(transform.failuresTag).setCoder(inputCollection.getCollection().getCoder());
                        results.put(name, FCollection.of(config.getName(), output, DataType.STRUCT, inputCollection.getSpannerType()));
                        results.put(name + OUTPUT_SUFFIX_FAILURES, FCollection.of(config.getName(), failures, DataType.STRUCT, inputCollection.getSpannerType()));
                    }
                    break;
                }
                case ENTITY: {
                    final FCollection<Entity> inputCollection = (FCollection<Entity>) input;
                    if("decrypt".equals(parameters.getMode().trim().toLowerCase())) {
                        final Decrypt<Entity> transform = new Decrypt<>(
                                parameters,
                                EntitySchemaUtil::getBytes,
                                (Entity entity, Map<String, byte[]> decryptedBytes) -> {
                                    final Entity.Builder builder = EntitySchemaUtil.toBuilder(entity, null, decryptedBytes.keySet());
                                    for (var entry : decryptedBytes.entrySet()) {
                                        builder.putProperties(entry.getKey(), Value.newBuilder().setBlobValue(ByteString.copyFrom(entry.getValue())).build());
                                    }
                                    return builder.build();
                                });
                        final PCollectionTuple outputs = inputCollection.getCollection().apply(name, transform);
                        final PCollection<?> output = outputs.get(transform.outputTag);
                        final PCollection<?> failures = outputs.get(transform.failuresTag).setCoder(inputCollection.getCollection().getCoder());
                        results.put(name, FCollection.of(config.getName(), output, DataType.ENTITY, inputCollection.getSpannerType()));
                        results.put(name + OUTPUT_SUFFIX_FAILURES, FCollection.of(config.getName(), failures, DataType.ENTITY, inputCollection.getAvroSchema()));
                    }
                    break;
                }
                default:
                    break;
            }
        }

        return results;
    }

    private static void validateParameters(final CryptoTransformParameters parameters) {
        if(parameters == null) {
            throw new IllegalArgumentException("CryptoTransform config parameters must not be empty!");
        }

        final List<String> errorMessages = new ArrayList<>();
        if(parameters.getMode() == null) {
            errorMessages.add("CryptoTransform config parameters `mode` must be specified decrypt or encrypt.");
        } else if(!parameters.getMode().trim().toLowerCase().equals("decrypt")) {
            errorMessages.add("CryptoTransform config parameters `mode` must be specified decrypt. but: " + parameters.getMode());
        }
        if(parameters.getFields() == null || parameters.getFields().size() == 0) {
            errorMessages.add("CryptoTransform config parameters `fields` must be specified.");
        }

        if(parameters.getKeyProvider() == null) {
            errorMessages.add("CryptoTransform config parameters `keyProvider` must be specified.");
        }
        if(parameters.getKeyProvider() != null) {
            if(parameters.getKeyProvider().getBase64text() == null && parameters.getKeyProvider().getVault() == null) {
                errorMessages.add("CryptoTransform config parameters `keyProvider.base64text` or `keyProvider.vault` must be specified.");
            }
            if(parameters.getKeyProvider().getVault() != null && parameters.getVault() == null) {
                errorMessages.add("If CryptoTransform config parameters `keyProvider.vault` specified, `vault` parameter must be specified.");
            }
        }

        if(parameters.getKeyDecryptor() != null) {
            if(parameters.getKeyDecryptor().getVault() != null && parameters.getVault() == null) {
                errorMessages.add("If CryptoTransform config parameters `keyDecryptor.vault` specified, `vault` parameter must be specified.");
            }
        }

        if(parameters.getVault() != null) {
            if(parameters.getVault().getHost() == null) {
                errorMessages.add("If CryptoTransform config parameters `vault` specified, `vault.host` must be specified.");
            }
            if(parameters.getVault().getRole() == null) {
                errorMessages.add("If CryptoTransform config parameters `vault` specified, `vault.role` must be specified.");
            }
        }

        if(errorMessages.size() > 0) {
            throw new IllegalArgumentException(String.join("\n", errorMessages));
        }
    }

    private static void setDefaultParameters(CryptoTransformParameters parameters) {
        if(parameters.getFailFast() == null) {
            parameters.setFailFast(true);
        }
    }

    public static class Decrypt<T> extends PTransform<PCollection<T>, PCollectionTuple> {

        private final TupleTag<T> outputTag = new TupleTag<>(){};
        private final TupleTag<T> failuresTag = new TupleTag<>(){};

        private final CryptoTransformParameters parameters;

        private final FieldGetter<T> getter;
        private final FieldSetter<T> setter;

        private Decrypt(final CryptoTransformParameters parameters,
                        final FieldGetter<T> getter,
                        final FieldSetter<T> setter) {

            this.parameters = parameters;
            this.getter = getter;
            this.setter = setter;
        }

        @Override
        public PCollectionTuple expand(final PCollection<T> input) {

            final DecryptDoFn dofn = new DecryptDoFn(
                    parameters.getFields(),
                    parameters.getAlgorithm(),
                    OptionUtil.getServiceAccount(),
                    parameters.getFailFast(),
                    parameters.getKeyProvider(),
                    parameters.getKeyDecryptor(),
                    parameters.getKeyExtractor(),
                    getter,
                    setter,
                    parameters.getVault());

            return input.apply("Decrypt", ParDo
                    .of(dofn)
                    .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
        }

        private class DecryptDoFn extends DoFn<T, T> {

            // Common required parameters
            private final List<String> fields;
            private final String serviceAccount;
            private final Boolean failFast;

            // Key process parameters
            private final CryptoTransformParameters.KeyProviderParameter keyProviderParameter;
            private final CryptoTransformParameters.KeyDecryptorParameter keyDecryptorParameter;
            private final CryptoTransformParameters.KeyExtractorParameter keyExtractorParameter;

            private final FieldGetter<T> getter;
            private final FieldSetter<T> setter;

            // Hashicorp.Vault parameters
            private final CryptoTransformParameters.VaultParameter vaultParameter;

            // Runtime variables
            private transient byte[] keyBytes;
            private transient String algorithm;
            private transient Object secret;

            private transient CryptoUtil crypto;
            private transient Template templateKeyExtractor;

            DecryptDoFn(final List<String> fields,
                        final String algorithm,
                        final String serviceAccount,
                        final Boolean failFast,
                        final CryptoTransformParameters.KeyProviderParameter keyProvider,
                        final CryptoTransformParameters.KeyDecryptorParameter keyDecryptor,
                        final CryptoTransformParameters.KeyExtractorParameter keyExtractor,
                        final FieldGetter<T> getter,
                        final FieldSetter<T> setter,
                        final CryptoTransformParameters.VaultParameter vaultParameter) {

                this.fields = fields;
                this.algorithm = algorithm;
                this.serviceAccount = serviceAccount;
                this.failFast = failFast;

                this.keyProviderParameter = keyProvider;
                this.keyDecryptorParameter = keyDecryptor;
                this.keyExtractorParameter = keyExtractor;

                this.getter = getter;
                this.setter = setter;

                this.vaultParameter = vaultParameter;
            }

            @Setup
            public void setup() throws Exception {
                this.crypto = new CryptoUtil();

                // KeyProvide
                final JsonElement keyProvider;
                if(keyProviderParameter.getBase64text() != null) {
                    keyProvider = JsonUtil.fromJson(keyProviderParameter.getBase64text());
                } else if(keyProviderParameter.getVault() != null) {
                    var vaultKeyProvider = keyProviderParameter.getVault();
                    var client = new VaultClient(vaultParameter.getHost(), serviceAccount, vaultParameter.getNamespace(), vaultParameter.getRole(), vaultParameter.getTokenPath());
                    String kvPath = TemplateUtil.executeStrictTemplate(vaultKeyProvider.getKvPath(), new HashMap<>());
                    keyProvider = client.readKVSecret(kvPath);
                    client.revokeToken();
                } else {
                    throw new IllegalArgumentException("");
                }

                // KeyDecryption
                final JsonElement decryptedKey;
                if(keyDecryptorParameter == null) {
                    decryptedKey = keyProvider;
                } else if(keyDecryptorParameter.getVault() != null) {
                    var vaultKeyDecriptor = keyDecryptorParameter.getVault();
                    var client = new VaultClient(vaultParameter.getHost(), serviceAccount, vaultParameter.getNamespace(), vaultParameter.getRole(), vaultParameter.getTokenPath());
                    if(keyDecryptorParameter.getJsonPath() != null) {
                        final Map<String, Object> data = JsonToMapConverter.convert(keyProvider);
                        final String path = TemplateUtil.executeStrictTemplate(vaultKeyDecriptor.getTransitPath(), data);
                        decryptedKey = client.decryptSecrets(path, keyDecryptorParameter.getJsonPath(), keyProvider);
                    } else {
                        final String decryptedString = client.decryptSecret(vaultKeyDecriptor.getTransitPath(), keyProvider.getAsString());
                        decryptedKey = JsonUtil.fromJson(decryptedString);
                    }
                    client.revokeToken();
                } else {
                    decryptedKey = keyProvider;
                }

                // KeyExtraction
                final String extractedKey;
                if(keyExtractorParameter == null) {
                    extractedKey = decryptedKey.getAsString();
                } else if(keyExtractorParameter.getTemplate() != null) {
                    if(keyExtractorParameter.getTemplate().startsWith("gs://")) {
                        this.templateKeyExtractor = TemplateUtil
                                .createStrictTemplate("config", StorageUtil
                                        .readString(keyExtractorParameter.getTemplate()));
                    } else {
                        this.templateKeyExtractor = TemplateUtil
                                .createStrictTemplate("config", keyExtractorParameter.getTemplate());
                    }
                    extractedKey = null;
                } else if(keyExtractorParameter.getJsonPath() != null) {
                    final List<KV<String, Object>> data = JsonUtil.read(decryptedKey.getAsJsonObject(), keyExtractorParameter.getJsonPath());
                    if(data.size() != 1) {
                        throw new IllegalStateException();
                    }
                    extractedKey = data.get(0).getValue().toString();
                } else {
                    extractedKey = decryptedKey.getAsString();
                }

                // Set Key
                if(extractedKey != null) {
                    this.keyBytes = Base64.getDecoder().decode(extractedKey);
                    this.secret = null;
                } else {
                    this.keyBytes = null;
                    if(decryptedKey.isJsonPrimitive()) {
                        this.secret = decryptedKey.getAsString();
                    } else {
                        this.secret = JsonToMapConverter.convert(decryptedKey);
                    }
                }
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final T input = c.element();
                final Map<String, byte[]> decryptedValues = new HashMap<>();
                try {
                    for (final String field : fields) {
                        final byte[] encryptedBytes = getter.getBytes(input, field);
                        final byte[] decryptedBytes;
                        if (templateKeyExtractor != null) {
                            final Map<String, Object> data = new HashMap<>();
                            data.put("_Crypto", crypto);
                            data.put("_algorithm", algorithm);
                            data.put("_secret", secret);
                            data.put("_value", encryptedBytes);
                            final String decryptedText = TemplateUtil.executeStrictTemplate(templateKeyExtractor, data).trim();
                            decryptedBytes = Base64.getDecoder().decode(decryptedText);
                        } else {
                            decryptedBytes = crypto.decrypt(algorithm, keyBytes, encryptedBytes);
                        }
                        decryptedValues.put(field, decryptedBytes);
                    }
                    final T output = setter.setValues(input, decryptedValues);
                    c.output(output);
                } catch (Exception e) {
                    final String message = "Failed to decrypt record: " + input + ", cause: " + e.toString();
                    if(failFast) {
                        throw new IllegalStateException(message, e);
                    } else {
                        LOG.error(message);
                        c.output(failuresTag, input);
                    }
                }
            }

        }

    }

    private interface FieldGetter<T> extends Serializable {
        byte[] getBytes(final T value, final String field);
    }

    private interface FieldSetter<T> extends Serializable {
        T setValues(final T element, final Map<String, byte[]> values);
    }

}
