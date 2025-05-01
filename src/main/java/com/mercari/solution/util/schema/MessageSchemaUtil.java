package com.mercari.solution.util.schema;

import com.google.gson.JsonObject;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MessageSchemaUtil {

    public static String getAsString(PubsubMessage message, String field) {
        if(field.startsWith("attributes")) {
            final String key = field.replaceFirst("attributes", "");
            return message.getAttributeMap().get(key);
        }
        return switch (field.toLowerCase()) {
            case "messageid" -> message.getMessageId();
            case "orderingkey" -> message.getOrderingKey();
            case "topic" -> message.getTopic();
            case "payload" -> new String(message.getPayload(), StandardCharsets.UTF_8);
            default -> null;
        };
    }

    public static ByteBuffer getAsBytes(PubsubMessage message, String field) {
        if(field.equalsIgnoreCase("payload")) {
            return ByteBuffer.wrap(message.getPayload());
        }
        final String str = switch (field.toLowerCase()) {
            case "messageid" -> message.getMessageId();
            case "orderingkey" -> message.getOrderingKey();
            case "topic" -> message.getTopic();
            default -> {
                if(field.startsWith("attributes.")) {
                    final String key = field.replaceFirst("attributes", "");
                    yield message.getAttribute(key);
                } else {
                    yield null;
                }
            }
        };
        return Optional
                .ofNullable(str)
                .map(String::getBytes)
                .map(ByteBuffer::wrap)
                .orElse(null);
    }

    public static Map<String, Object> asPrimitiveMap(PubsubMessage message) {
        final Map<String, Object> primitiveMap = new HashMap<>();
        if(message == null) {
            return primitiveMap;
        }
        primitiveMap.put("messageId", message.getMessageId());
        primitiveMap.put("orderingKey", message.getOrderingKey());
        primitiveMap.put("topic", message.getTopic());
        primitiveMap.put("payload", ByteBuffer.wrap(message.getPayload()));
        primitiveMap.put("attributes", message.getAttributeMap());
        return primitiveMap;
    }

    public static Object getAsPrimitive(PubsubMessage message, String field) {
        if(message == null || field == null) {
            return null;
        }
        return switch (field) {
            case "messageId" -> message.getMessageId();
            case "orderingKey" -> message.getOrderingKey();
            case "topic" -> message.getTopic();
            case "payload" -> ByteBuffer.wrap(message.getPayload());
            case "attributes" -> message.getAttributeMap();
            default -> null;
        };
    }

    public static String toJsonString(final PubsubMessage message) {
        if(message == null) {
            return null;
        }
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("messageId", message.getMessageId());
        jsonObject.addProperty("topic", message.getTopic());
        jsonObject.addProperty("orderingKey", message.getOrderingKey());
        jsonObject.addProperty("payload", Base64.getEncoder().encodeToString(message.getPayload()));
        if(message.getAttributeMap() != null) {
            final JsonObject attributes = new JsonObject();
            for(final Map.Entry<String, String> entry : message.getAttributeMap().entrySet()) {
                attributes.addProperty(entry.getKey(), entry.getValue());
            }
            jsonObject.add("attributes", attributes);
        }
        return jsonObject.toString();
    }

}
