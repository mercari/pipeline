package com.mercari.solution.util.schema;

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
            case "payload" -> Base64.getEncoder().encodeToString(message.getPayload());
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
                .map(s -> Base64.getDecoder().decode(s))
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

}
