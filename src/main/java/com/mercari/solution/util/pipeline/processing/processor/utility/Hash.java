package com.mercari.solution.util.pipeline.processing.processor.utility;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.pipeline.Filter;
import com.mercari.solution.util.gcp.SecretManagerUtil;
import com.mercari.solution.util.pipeline.processing.ProcessingBuffer;
import com.mercari.solution.util.pipeline.processing.ProcessingState;
import com.mercari.solution.util.pipeline.processing.processor.Processor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class Hash implements Processor {

    private static final String ALGORITHM_HMAC_SHA256 = "HmacSHA256";

    private final String name;
    private final String algorithm;
    private final String mode;
    private final List<String> fields;
    private final String secret;
    private final Integer size;

    private final String condition;
    private final Boolean ignore;
    private final Boolean isSingleField;


    private final Map<String, Integer> bufferSizes;

    private transient Filter.ConditionNode conditionNode;

    private transient Mac mac;


    public Hash(final String name, final String condition, final Boolean ignore, final JsonObject params) {

        this.name = name;
        this.condition = condition;
        this.ignore = ignore != null && ignore;

        if(params.has("mode") && params.get("mode").isJsonPrimitive()) {
            this.mode = params.get("mode").getAsString();
            if(!this.mode.equals("string") && !this.mode.equals("bytes")) {
                throw new IllegalArgumentException("Hash step: " + name + " mode value must be string or bytes");
            }
        } else {
            this.mode = "string";
        }

        if(!params.has("algorithm") || !params.get("algorithm").isJsonPrimitive()) {
            throw new IllegalArgumentException("Hash step: " + name + " requires parameter algorithm");
        }
        this.algorithm = params.get("algorithm").getAsString();

        if(params.has("size") && params.get("size").isJsonPrimitive()) {
            this.size = params.get("size").getAsInt();
        } else {
            this.size = null;
        }

        if(params.has("secret")) {
            this.secret = params.get("secret").getAsString();
        } else {
            switch (algorithm) {
                case ALGORITHM_HMAC_SHA256: {
                    throw new IllegalArgumentException("Hash step: " + name + " requires parameter secret if algorithm is " + algorithm);
                }
                default:
                    this.secret = null;
            }
        }

        if(params.has("field") || params.has("fields")) {
            final KV<List<String>,Boolean> fieldsAndIsSingle = Processor
                    .getSingleMultiAttribute(params, "field", "fields");
            this.fields = fieldsAndIsSingle.getKey();
            this.isSingleField = fieldsAndIsSingle.getValue();
        } else {
            throw new IllegalArgumentException("Hash step: " + name + " requires parameter field or fields");
        }

        this.bufferSizes = new HashMap<>();
    }

    public static Hash of(final String name, final String condition, final Boolean ignore, final JsonObject params) {
        return new Hash(name, condition, ignore, params);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Op getOp() {
        return Op.hash;
    }

    @Override
    public boolean ignore() {
        return ignore;
    }

    @Override
    public boolean filter(Map<String, Object> input) {
        if(condition == null) {
            return true;
        }
        return Filter.filter(this.conditionNode, input);
    }

    @Override
    public SizeUnit getSizeUnit() {
        return SizeUnit.count;
    }

    @Override
    public List<Schema.Field> getOutputFields(Map<String, Schema.FieldType> inputTypes) {
        final Schema.FieldType fieldType = Schema.FieldType.STRING.withNullable(true);
        final List<Schema.Field> outputFields = new ArrayList<>();
        outputFields.add(Schema.Field.of(name, fieldType));
        return outputFields;
    }

    @Override
    public Map<String, Schema.FieldType> getBufferTypes(Map<String, Schema.FieldType> inputTypes) {
        final Map<String, Schema.FieldType> bufferTypes = new HashMap<>();
        for(final String input : bufferSizes.keySet()) {
            final Schema.FieldType inputType = inputTypes.get(input);
            if(inputType != null) {
                bufferTypes.put(input, inputType);
            } else if(name.equals(input)) {
                bufferTypes.put(input, Schema.FieldType.DOUBLE.withNullable(true));
            } else {
                throw new IllegalArgumentException("expression processor: " + name + " has no inputType for input: " + input);
            }
        }
        return bufferTypes;
    }

    @Override
    public Map<String, Integer> getBufferSizes() {
        return this.bufferSizes;
    }

    @Override
    public void setup() {
        if(this.condition != null && this.conditionNode == null) {
            this.conditionNode = Filter.parse(new Gson().fromJson(this.condition, JsonElement.class));
        }

        try {
            switch (algorithm) {
                case ALGORITHM_HMAC_SHA256: {
                    this.mac = Mac.getInstance(algorithm);
                    final String secret;
                    if(SecretManagerUtil.isSecretName(this.secret)) {
                        secret = SecretManagerUtil.getSecret(this.secret).toStringUtf8();
                    } else {
                        secret = this.secret;
                    }
                    final SecretKeySpec secretKeySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), algorithm);
                    this.mac.init(secretKeySpec);
                    break;
                }
                default:
                    throw new IllegalArgumentException("hash algorithm: " + algorithm + " is not supported");
            }
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("Not supported algorithm error: ", e);
        } catch (final InvalidKeyException e) {
            throw new IllegalArgumentException("Invalid key error for secret: " + this.secret, e);
        }

    }

    @Override
    public Map<String, Object> process(ProcessingBuffer buffer, ProcessingState state, Instant timestamp) {
        final Map<String, Object> input = buffer.getLatestValues();
        final Map<String, Object> outputs = hash(input);
        return outputs;
    }

    @Override
    public Map<String, Object> process(Map<String, Object> input, Instant timestamp) {
        final Map<String, Object> outputs = hash(input);
        return outputs;
    }

    @Override
    public List<Map<String, Object>> process(List<Map<String, Object>> inputs, Instant timestamp) {
        final List<Map<String, Object>> outputs = new ArrayList<>();
        for(final Map<String, Object> input : inputs) {
            final Map<String, Object> output = hash(input);
            outputs.add(output);
        }
        return outputs;
    }

    private Map<String, Object> hash(Map<String, Object> input) {
        final Map<String, Object> outputs = new HashMap<>();
        for(final String field : fields) {
            final Object value = input.get(field);
            final String outputName = isSingleField ? name : String.format("%s_%s", name, field);
            if(value == null) {
                outputs.put(outputName, null);
            } else {
                final byte[] bytes;
                if(value instanceof String) {
                    bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                } else if(value instanceof byte[]) {
                    bytes = (byte[]) value;
                } else {
                    bytes = null;
                }
                if("string".equals(mode)) {
                    final String output = hashAsString(bytes);
                    outputs.put(outputName, output);
                } else if("bytes".equals(mode)) {

                } else {
                    throw new IllegalArgumentException();
                }
            }
        }

        return outputs;
    }

    private String hashAsString(final byte[] bytes) {
        if(bytes == null) {
            return null;
        }
        final String output;
        switch (algorithm) {
            case ALGORITHM_HMAC_SHA256:
                output = hashAsStringHMACSHA256(bytes);
                break;
            default:
                throw new IllegalArgumentException();
        }

        if(size == null) {
            return output;
        } else if(size > output.length()) {
            return output;
        } else {
            return output.substring(0, size);
        }
    }

    private String hashAsStringHMACSHA256(final byte[] bytes) {
        final byte[] output = mac.doFinal(bytes);
        StringBuilder sb = new StringBuilder(output.length * 2);
        for(byte b : output) {
            sb.append(String.format("%02x", b&0xff));
        }
        return sb.toString();
    }

}
