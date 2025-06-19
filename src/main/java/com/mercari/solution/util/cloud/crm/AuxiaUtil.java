package com.mercari.solution.util.cloud.crm;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.internal.LazilyParsedNumber;
import com.google.protobuf.*;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.ProtoSchemaUtil;
import org.apache.commons.lang3.math.NumberUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.*;

public class AuxiaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(AuxiaUtil.class);

    private static final String RESOURCE_EVENT_DESCRIPTOR_PATH = "/schema/protobuf/auxia.desc";
    private static final String RESOURCE_RUNTIME_EVENT_DESCRIPTOR_PATH = "/template/MPipeline/resources/schema/protobuf/auxia.desc";
    //private static final String RESOURCE_RUNTIME_EVENT_DESCRIPTOR_PATH = "/app/resources/schema/protobuf/auxia.desc";

    private static final Set<String> PROPERTIES_MESSAGE = Set.of(
            "user_id", "project_id", "events", "event"
    );
    private static final Set<String> PROPERTIES_EVENT = Set.of(
            "event_name","insert_id","client_event_timestamp","server_received_timestamp",
            "event_properties","user_properties",
            "pre_login_temp_user_id","session_id","country","region","city",
            "ip_address","device_id","app_version_id"
    );

    public enum Type {
        json,
        element
    }

    public static Map<String, Descriptors.Descriptor> getMessageDescriptors() {
        try (final InputStream is = AuxiaUtil.class.getResourceAsStream(RESOURCE_EVENT_DESCRIPTOR_PATH)) {
            return ProtoSchemaUtil.getDescriptors(is);
        } catch (final IOException e) {
            throw new IllegalArgumentException("Not found event descriptor file", e);
        }
    }

    public static DescriptorProtos.FileDescriptorSet getFileDescriptorSet() {
        try (final InputStream is = AuxiaUtil.class.getResourceAsStream(RESOURCE_EVENT_DESCRIPTOR_PATH)) {
            if(is == null) {
                LOG.info("Auxia protobuf descriptor file is not found: " + RESOURCE_EVENT_DESCRIPTOR_PATH);
                try(final InputStream iss = Files.newInputStream(Path.of(RESOURCE_RUNTIME_EVENT_DESCRIPTOR_PATH))) {
                    return ProtoSchemaUtil.getFileDescriptorSet(iss);
                } catch (Throwable e) {
                    throw new IllegalArgumentException("Auxia protobuf descriptor file is not found");
                }
            }
            return ProtoSchemaUtil.getFileDescriptorSet(is);
        } catch (final IOException e) {
            throw new IllegalArgumentException("Not found event descriptor file", e);
        }
    }

    public static EventElementConverter createElementConverter(
            final String projectId,
            final String eventName,
            final Schema schema,
            final Set<String> excludeFields) {
        return new EventElementConverter(projectId, eventName, schema, excludeFields);
    }

    public static EventJsonConverter createJsonConverter(
            final String projectId,
            final String eventName,
            final String field,
            final Set<String> excludeFields) {
        return new EventJsonConverter(projectId, eventName, field, excludeFields);
    }

    public static class Event {

        public String userId;
        public String eventName;
        public String insertId;
        public Long timestampMicros;
        public DynamicMessage message;

        public static Event of(
                final DynamicMessage message,
                final String userId,
                final String eventName,
                final String insertId,
                final Long timestampMicros) {

            final Event event = new Event();
            event.message = message;
            event.userId = userId;
            event.eventName = eventName;
            event.insertId = insertId;
            event.timestampMicros = timestampMicros;
            return event;
        }
    }

    public static abstract class EventConverter {

        protected final String projectId;
        protected final String eventName;
        protected final Set<String> excludeFields;

        // LogEventsRequest
        protected transient Descriptors.Descriptor message;
        protected transient Descriptors.FieldDescriptor projectIdField;
        protected transient Descriptors.FieldDescriptor userIdField;
        protected transient Descriptors.FieldDescriptor eventsField;

        // Event
        protected transient Descriptors.Descriptor eventMessage;
        protected transient Descriptors.FieldDescriptor eventNameField;
        protected transient Descriptors.FieldDescriptor insertIdField;
        protected transient Descriptors.FieldDescriptor eventPropertiesField;
        protected transient Descriptors.FieldDescriptor userPropertiesField;
        protected transient Descriptors.FieldDescriptor clientEventTimestampField;
        protected transient Descriptors.FieldDescriptor serverReceivedTimestampField;
        protected transient Descriptors.FieldDescriptor preLoginTempUserIdField;
        protected transient Descriptors.FieldDescriptor sessionIdField;
        protected transient Descriptors.FieldDescriptor countryField;
        protected transient Descriptors.FieldDescriptor regionField;
        protected transient Descriptors.FieldDescriptor cityField;
        protected transient Descriptors.FieldDescriptor ipAddressField;
        protected transient Descriptors.FieldDescriptor deviceIdField;
        protected transient Descriptors.FieldDescriptor appVersionIdField;

        // EventPropertiesEntry
        protected transient Descriptors.Descriptor eventPropertiesMessage;
        protected transient Descriptors.FieldDescriptor eventPropertiesKeyField;
        protected transient Descriptors.FieldDescriptor eventPropertiesValueField;

        // UserPropertiesEntry
        protected transient Descriptors.Descriptor userPropertiesMessage;
        protected transient Descriptors.FieldDescriptor userPropertiesKeyField;
        protected transient Descriptors.FieldDescriptor userPropertiesValueField;

        // PropertyValue
        protected transient Descriptors.Descriptor propertyValueMessage;
        protected transient Descriptors.FieldDescriptor propertyValueLongField;
        protected transient Descriptors.FieldDescriptor propertyValueDoubleField;
        protected transient Descriptors.FieldDescriptor propertyValueStringField;
        protected transient Descriptors.FieldDescriptor propertyValueBooleanField;
        protected transient Descriptors.FieldDescriptor propertyValueTimestampField;

        protected EventConverter(
                final String projectId,
                final String eventName,
                final Set<String> excludeFields) {
            this.projectId = projectId;
            this.eventName = eventName;
            this.excludeFields = excludeFields;
        }

        public void setup() {
            final byte[] bytes = getFileDescriptorSet().toByteArray();
            setup(bytes);
        }

        public void setup(final byte[] bytes) {
            final DescriptorProtos.FileDescriptorSet set;
            try {
                set = DescriptorProtos.FileDescriptorSet
                        .parseFrom(bytes);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            final Map<String, Descriptors.Descriptor> messageDescriptors = ProtoSchemaUtil.getDescriptors(set);

            this.message = messageDescriptors.get("auxia.event.v1.LogEventsRequest");
            if(message == null) {
                throw new RuntimeException("not message: " + messageDescriptors);
            }
            this.projectIdField = AuxiaUtil.getField(message, "project_id");
            this.userIdField = AuxiaUtil.getField(message, "user_id");
            this.eventsField = AuxiaUtil.getField(message, "events");

            this.eventMessage = messageDescriptors.get("auxia.event.v1.Event");
            this.eventNameField = AuxiaUtil.getField(eventMessage, "event_name");
            this.insertIdField = AuxiaUtil.getField(eventMessage, "insert_id");
            this.eventPropertiesField = AuxiaUtil.getField(eventMessage, "event_properties");
            this.userPropertiesField = AuxiaUtil.getField(eventMessage, "user_properties");
            this.clientEventTimestampField = AuxiaUtil.getField(eventMessage, "client_event_timestamp");
            this.serverReceivedTimestampField = AuxiaUtil.getField(eventMessage, "server_received_timestamp");
            this.preLoginTempUserIdField = AuxiaUtil.getField(eventMessage, "pre_login_temp_user_id");
            this.sessionIdField = AuxiaUtil.getField(eventMessage, "session_id");
            this.countryField = AuxiaUtil.getField(eventMessage, "country");
            this.regionField = AuxiaUtil.getField(eventMessage, "region");
            this.cityField = AuxiaUtil.getField(eventMessage, "city");
            this.ipAddressField = AuxiaUtil.getField(eventMessage, "ip_address");
            this.deviceIdField = AuxiaUtil.getField(eventMessage, "device_id");
            this.appVersionIdField = AuxiaUtil.getField(eventMessage, "app_version_id");

            this.eventPropertiesMessage = messageDescriptors.get("auxia.event.v1.Event.EventPropertiesEntry");
            this.eventPropertiesKeyField = AuxiaUtil.getField(eventPropertiesMessage, "key");
            this.eventPropertiesValueField = AuxiaUtil.getField(eventPropertiesMessage, "value");

            this.userPropertiesMessage = messageDescriptors.get("auxia.event.v1.Event.UserPropertiesEntry");
            this.userPropertiesKeyField = AuxiaUtil.getField(userPropertiesMessage, "key");
            this.userPropertiesValueField = AuxiaUtil.getField(userPropertiesMessage, "value");

            this.propertyValueMessage = messageDescriptors.get("auxia.event.v1.PropertyValue");
            this.propertyValueLongField = AuxiaUtil.getField(propertyValueMessage, "long_value");
            this.propertyValueDoubleField = AuxiaUtil.getField(propertyValueMessage, "double_value");
            this.propertyValueStringField = AuxiaUtil.getField(propertyValueMessage, "string_value");
            this.propertyValueBooleanField = AuxiaUtil.getField(propertyValueMessage, "boolean_value");
            this.propertyValueTimestampField = AuxiaUtil.getField(propertyValueMessage, "timestamp_value");
        }

        protected static DynamicMessage toTimestamp(final JsonElement jsonElement) {
            if(jsonElement.isJsonPrimitive()) {
                final JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();
                final Timestamp timestamp;
                if(jsonPrimitive.isString()) {
                    timestamp = DateTimeUtil.toProtoTimestamp(jsonPrimitive.getAsString());
                } else if(jsonPrimitive.isNumber()) {
                    timestamp = DateTimeUtil.toProtoTimestamp(jsonPrimitive.getAsLong());
                } else {
                    throw new IllegalArgumentException("timestamp value is illegal: " + jsonPrimitive);
                }
                return toTimestamp(timestamp);
            } else if(jsonElement.isJsonObject()) {
                return toTimestamp(jsonElement.getAsJsonObject());
            } else if(!jsonElement.isJsonNull()) {
                throw new IllegalArgumentException("timestamp value is illegal: " + jsonElement);
            } else {
                return null;
            }
        }

        protected static DynamicMessage toTimestamp(final String timestampText) {
            final Timestamp timestamp = DateTimeUtil.toProtoTimestamp(timestampText);
            return toTimestamp(timestamp);
        }

        protected static DynamicMessage toTimestamp(final Long epochMicros) {
            final Timestamp timestamp = DateTimeUtil.toProtoTimestamp(epochMicros);
            return toTimestamp(timestamp);
        }

        protected static DynamicMessage toTimestamp(final Integer epochDays) {
            final java.time.Instant timestamp = DateTimeUtil.toInstant(LocalDate.ofEpochDay(epochDays).toString());
            return toTimestamp(timestamp);
        }

        protected static DynamicMessage toTimestamp(final java.time.Instant instant) {
            return DynamicMessage.newBuilder(Timestamp.getDescriptor())
                    .setField(Timestamp.getDescriptor().findFieldByName("seconds"), instant.getEpochSecond())
                    .setField(Timestamp.getDescriptor().findFieldByName("nanos"), instant.getNano())
                    .build();
        }

        protected static DynamicMessage toTimestamp(final Timestamp timestamp) {
            return DynamicMessage.newBuilder(Timestamp.getDescriptor())
                    .setField(Timestamp.getDescriptor().findFieldByName("seconds"), timestamp.getSeconds())
                    .setField(Timestamp.getDescriptor().findFieldByName("nanos"), timestamp.getNanos())
                    .build();
        }

        protected static DynamicMessage toTimestamp(final long seconds, final int nano) {
            return DynamicMessage.newBuilder(Timestamp.getDescriptor())
                    .setField(Timestamp.getDescriptor().findFieldByName("seconds"), seconds)
                    .setField(Timestamp.getDescriptor().findFieldByName("nanos"), nano)
                    .build();
        }

        protected static DynamicMessage toTimestamp(final JsonObject jsonObject) {
            if(jsonObject.size() == 2 && jsonObject.has("seconds") && jsonObject.has("nanos")) {
                final JsonElement secondsElement = jsonObject.get("seconds");
                final JsonElement nanosElement = jsonObject.get("nanos");
                if(!secondsElement.isJsonPrimitive() || !secondsElement.getAsJsonPrimitive().isNumber()) {
                    throw new IllegalArgumentException("timestamp value is illegal. seconds: " + secondsElement + ", nanos: " + nanosElement);
                }
                if(!nanosElement.isJsonPrimitive() || !nanosElement.getAsJsonPrimitive().isNumber()) {
                    throw new IllegalArgumentException("timestamp value is illegal. seconds: " + secondsElement + ", nanos: " + nanosElement);
                }
                final long seconds = secondsElement.getAsLong();
                final int nanos = nanosElement.getAsInt();
                return  toTimestamp(seconds, nanos);
            } else {
                throw new IllegalArgumentException("timestamp value is illegal: " + jsonObject);
            }
        }

        public static Long toEpochMicros(final DynamicMessage timestamp) {
            final long seconds = (Long) Optional.ofNullable(timestamp.getField(Timestamp.getDescriptor().findFieldByName("seconds"))).orElse(0L);
            final int nanos = (Integer) Optional.ofNullable(timestamp.getField(Timestamp.getDescriptor().findFieldByName("nanos"))).orElse(0);
            return seconds * 1000_000 + nanos / 1000;
        }

        public static Long toEpochMillis(final DynamicMessage timestamp) {
            return toEpochMicros(timestamp) / 1000L;
        }

        abstract public List<Event> convert(final MElement element, final Instant timestamp);

        abstract public List<String> validate(final Schema schema);

    }

    public static class EventElementConverter extends EventConverter {

        private final Schema schema;

        EventElementConverter(
                final String projectId,
                final String eventName,
                final Schema schema,
                final Set<String> excludeFields) {

            super(projectId, eventName, excludeFields);
            this.schema = schema;
        }

        @Override
        public List<String> validate(final Schema schema) {
            final List<String> errorMessages = new ArrayList<>();
            if(schema.hasField("events")) {
                final Schema.Field field = schema.getField("events");
                if(!Schema.Type.array.equals(field.getFieldType().getType())) {
                    errorMessages.add("events must be array type: " + field.getFieldType().getType());
                } else if(!Schema.Type.element.equals(field.getFieldType().getArrayValueType().getType())) {
                    errorMessages.add("events array element must be element type: " + field.getFieldType().getType());
                }
            }
            return errorMessages;
        }

        @Override
        public List<Event> convert(final MElement element, final Instant timestamp) {
            if(element == null) {
                return new ArrayList<>();
            }

            final Map<String, Object> values = element.asPrimitiveMap();

            final List<DynamicMessage> eventMessages = new ArrayList<>();
            if(schema.hasField("events")) {
                final Schema.Field field = schema.getField("events");
                if(values.get(field.getName()) instanceof Iterable<?>) {
                    throw new IllegalArgumentException("event must be array value: " + values.get(field.getName()));
                }
                final Iterable<Map<String, Object>> array = (Iterable<Map<String, Object>>) element.getPrimitiveValue(field.getName());
                for(final Map<String, Object> v : array) {
                    final DynamicMessage eventMessage = createEvent(field.getFieldType().getArrayValueType().getElementSchema(), v, timestamp);
                    eventMessages.add(eventMessage);
                }
            } else {
                final DynamicMessage eventMessage = createEvent(schema, values, timestamp);
                eventMessages.add(eventMessage);
            }

            final String userId = element.getAsString("user_id");

            final List<Event> events = new ArrayList<>();
            for(final DynamicMessage eventMessage : eventMessages) {
                final DynamicMessage.Builder builder = DynamicMessage.newBuilder(message);
                builder.setField(projectIdField, projectId);
                builder.setField(userIdField, userId);
                builder.addRepeatedField(eventsField, eventMessage);
                final DynamicMessage message = builder.build();
                final String eventName = (String) eventMessage.getField(eventNameField);
                final String insertId = (String) eventMessage.getField(insertIdField);
                final DynamicMessage timestampMessage = (DynamicMessage) eventMessage.getField(clientEventTimestampField);
                final Long timestampMillis = toEpochMillis(timestampMessage);
                events.add(Event.of(message, userId, eventName, insertId, timestampMillis));
            }


            return events;
        }

        private DynamicMessage createEvent(final Schema schema, final Map<String, Object> values, final Instant eventTime) {
            final DynamicMessage.Builder builder = DynamicMessage.newBuilder(eventMessage);
            for(final Schema.Field field : schema.getFields()) {
                final Object value = values.get(field.getName());
                switch (field.getName()) {
                    case "event_name" -> builder.setField(eventNameField, getAsString(value));
                    case "insert_id" -> builder.setField(insertIdField, getAsString(value));
                    case "client_event_timestamp" -> {
                        final DynamicMessage timestamp = getAsTimestamp(value);
                        builder.setField(clientEventTimestampField, timestamp);
                    }
                    case "server_received_timestamp" -> {
                        final DynamicMessage timestamp = getAsTimestamp(value);
                        builder.setField(serverReceivedTimestampField, timestamp);
                    }
                    case "event_properties" -> {
                        if(!Schema.Type.element.equals(field.getFieldType().getType())) {
                            throw new IllegalArgumentException("event_properties type must be element");
                        }
                        if(!(value instanceof Map)) {
                            throw new IllegalArgumentException("event_properties value must be Map");
                        }
                        final List<DynamicMessage> eventPropertiesMessages = createEventProperties(field.getFieldType().getElementSchema(), (Map<String, Object>) value);
                        if(!eventPropertiesMessages.isEmpty()) {
                            builder.setField(eventPropertiesField, eventPropertiesMessages);
                        }
                    }
                    case "user_properties" -> {
                        if(!Schema.Type.element.equals(field.getFieldType().getType())) {
                            throw new IllegalArgumentException("user_properties type must be element");
                        }
                        if(!(value instanceof Map)) {
                            throw new IllegalArgumentException("user_properties must be Map");
                        }
                        final List<DynamicMessage> userPropertiesMessages = createUserProperties(field.getFieldType().getElementSchema(), (Map<String, Object>) value);
                        if(!userPropertiesMessages.isEmpty()) {
                            builder.setField(userPropertiesField, userPropertiesMessages);
                        }
                    }
                    case "pre_login_temp_user_id" -> builder.setField(preLoginTempUserIdField, getAsString(value));
                    case "session_id" -> builder.setField(sessionIdField, getAsString(value));
                    case "country" -> builder.setField(countryField, getAsString(value));
                    case "region" -> builder.setField(regionField, getAsString(value));
                    case "city" -> builder.setField(cityField, getAsString(value));
                    case "ip_address" -> builder.setField(ipAddressField, getAsString(value));
                    case "device_id" -> builder.setField(deviceIdField, getAsString(value));
                    case "app_version_id" -> builder.setField(appVersionIdField, getAsString(value));
                }
            }

            if(!builder.hasField(eventNameField)) {
                if(eventName == null) {
                    throw new IllegalArgumentException("event_name is not found");
                }
                builder.setField(eventNameField, eventName);
            }

            if(!builder.hasField(clientEventTimestampField)) {
                builder.setField(clientEventTimestampField, toTimestamp(eventTime.getMillis() * 1000L));
            }

            if(!values.containsKey("event_properties") && !values.containsKey("user_properties")) {
                final List<DynamicMessage> eventPropertiesMessages = createEventProperties(schema, values);
                if(!eventPropertiesMessages.isEmpty()) {
                    builder.setField(eventPropertiesField, eventPropertiesMessages);
                }
            }

            if(!builder.hasField(insertIdField)) {
                final int hash = builder.hashCode();
                builder.setField(insertIdField, Integer.toString(hash));
            }

            return builder.build();
        }

        private List<DynamicMessage> createEventProperties(
                final Schema schema,
                final Map<String, Object> map) {

            final List<DynamicMessage> eventProperties = new ArrayList<>();
            for(final Schema.Field field : schema.getFields()) {
                if(PROPERTIES_EVENT.contains(field.getName()) || PROPERTIES_MESSAGE.contains(field.getName())) {
                    continue;
                }
                if(excludeFields != null && !excludeFields.isEmpty()) {
                    if(excludeFields.contains(field.getName())) {
                        continue;
                    }
                }
                final Object value = map.get(field.getName());
                final DynamicMessage propertyValueMessage = createPropertyValue(field.getFieldType(), value);
                if(propertyValueMessage != null) {
                    final DynamicMessage.Builder builder = DynamicMessage.newBuilder(eventPropertiesMessage);
                    builder.setField(eventPropertiesKeyField, field.getName());
                    builder.setField(eventPropertiesValueField, propertyValueMessage);
                    final DynamicMessage eventProperty = builder.build();
                    eventProperties.add(eventProperty);
                }
            }
            return eventProperties;
        }

        private List<DynamicMessage> createUserProperties(
                final Schema schema,
                final Map<String, Object> map) {

            final List<DynamicMessage> userProperties = new ArrayList<>();
            for(final Schema.Field field : schema.getFields()) {
                if(PROPERTIES_EVENT.contains(field.getName()) || PROPERTIES_MESSAGE.contains(field.getName())) {
                    continue;
                }
                final Object value = map.get(field.getName());
                final DynamicMessage propertyValueMessage = createPropertyValue(field.getFieldType(), value);
                if(propertyValueMessage != null) {
                    final DynamicMessage.Builder builder = DynamicMessage.newBuilder(userPropertiesMessage);
                    builder.setField(userPropertiesKeyField, field.getName());
                    builder.setField(userPropertiesValueField, propertyValueMessage);
                    final DynamicMessage userProperty = builder.build();
                    userProperties.add(userProperty);
                }
            }
            return userProperties;
        }

        private DynamicMessage createPropertyValue(
                final Schema.FieldType fieldType,
                final Object value) {

            if(value == null) {
                return null;
            }
            final DynamicMessage.Builder builder = DynamicMessage.newBuilder(propertyValueMessage);
            switch (fieldType.getType()) {
                case bool -> builder.setField(propertyValueBooleanField, value);
                case string, json -> builder.setField(propertyValueStringField, value);
                case float32 -> builder.setField(propertyValueDoubleField, ((Float)value).doubleValue());
                case float64 -> builder.setField(propertyValueDoubleField, value);
                case int32 -> builder.setField(propertyValueLongField, ((Integer) value).longValue());
                case int64 -> builder.setField(propertyValueLongField, value);
                case timestamp -> {
                    final DynamicMessage timestamp = toTimestamp((Long) value);
                    builder.setField(propertyValueTimestampField, timestamp);
                }
                case date -> {
                    final DynamicMessage timestamp = toTimestamp((Integer) value);
                    builder.setField(propertyValueTimestampField, timestamp);
                }
                case enumeration -> {
                    final String symbol = fieldType.getSymbols().get((Integer)value);
                    builder.setField(propertyValueStringField, symbol);
                }
                default -> throw new IllegalArgumentException("Not supported: " + fieldType.getType() + " for value: " + value);
            }
            return builder.build();
        }

        private static String getAsString(final Object value) {
            return (String) MElement.getAsStandardValue(Schema.FieldType.STRING, value);
        }

        private static DynamicMessage getAsTimestamp(final Object value) {
            final java.time.Instant instant = (java.time.Instant) MElement
                    .getAsStandardValue(Schema.FieldType.TIMESTAMP, value);
            return toTimestamp(instant);
        }

    }

    public static class EventJsonConverter extends EventConverter {

        private final String field;

        protected EventJsonConverter(
                final String projectId,
                final String eventName,
                final String field,
                final Set<String> excludeFields) {

            super(projectId, eventName, excludeFields);
            this.field = field;
        }

        @Override
        public List<String> validate(final Schema schema) {
            return new ArrayList<>();
        }

        @Override
        public List<Event> convert(final MElement element, final Instant timestamp) {
            final String jsonString = element.getAsString(field);
            if(jsonString == null) {
                return new ArrayList<>();
            }

            final JsonElement jsonElement = new Gson().fromJson(jsonString, JsonElement.class);

            if(jsonElement.isJsonObject()) {
                final JsonObject jsonObject = jsonElement.getAsJsonObject();
                return convert(jsonObject, timestamp);
            } else if(jsonElement.isJsonArray()) {
                final List<Event> events = new ArrayList<>();
                for(final JsonElement e : jsonElement.getAsJsonArray()) {
                    if(e.isJsonObject()) {
                        final JsonObject jsonObject = e.getAsJsonObject();
                        final List<Event> es = convert(jsonObject, timestamp);
                        events.addAll(es);
                    } else if(!e.isJsonNull()) {
                        throw new IllegalArgumentException("Illegal json message value: " + e);
                    }
                }
                return events;
            } else if(jsonElement.isJsonNull()) {
                return new ArrayList<>();
            } else {
                throw new IllegalArgumentException("Illegal json message value: " + jsonElement);
            }
        }

        public List<Event> convert(final JsonObject jsonObject, final Instant timestamp) {
            if(jsonObject == null || !jsonObject.isJsonObject()) {
                throw new IllegalArgumentException("Illegal json object: " + jsonObject);
            }

            final List<DynamicMessage> eventMessages = new ArrayList<>();
            if(jsonObject.has("events")) {
                final JsonElement eventsElement = jsonObject.get("events");
                if(!eventsElement.isJsonArray()) {
                    throw new IllegalArgumentException("events must be jsonArray: " + jsonObject);
                }
                for(final JsonElement jsonElement : eventsElement.getAsJsonArray()) {
                    if(!jsonElement.isJsonObject()) {
                        throw new IllegalArgumentException("events element must be jsonObject: " + jsonObject);
                    }
                    final DynamicMessage eventMessage = createEvent(jsonElement.getAsJsonObject(), timestamp);
                    eventMessages.add(eventMessage);
                }
            } else {
                final DynamicMessage eventMessage = createEvent(jsonObject, timestamp);
                eventMessages.add(eventMessage);
            }

            final String userId = jsonObject.get("user_id").getAsString();

            final List<Event> events = new ArrayList<>();
            for(final DynamicMessage eventMessage :eventMessages) {
                final DynamicMessage.Builder builder = DynamicMessage.newBuilder(message);
                builder.setField(projectIdField, projectId);
                builder.setField(userIdField, userId);
                builder.addRepeatedField(eventsField, eventMessage);
                validate(builder, jsonObject);
                final DynamicMessage message = builder.build();
                final String eventName = (String) eventMessage.getField(eventNameField);
                final String insertId = (String) eventMessage.getField(insertIdField);
                final DynamicMessage timestampMessage = (DynamicMessage) eventMessage.getField(clientEventTimestampField);
                final Long timestampMillis = toEpochMillis(timestampMessage);
                events.add(Event.of(message, userId, eventName, insertId, timestampMillis));
            }

            return events;
        }

        private void validate(
                final DynamicMessage.Builder builder,
                final JsonObject jsonObject) {

            if(!builder.hasField(userIdField)) {
                throw new IllegalArgumentException("user_id not found in message: " + builder + " in jsonObject: " + jsonObject);
            }
        }

        private DynamicMessage createEvent(final JsonObject jsonObject, final Instant eventTime) {
            final DynamicMessage.Builder builder = DynamicMessage.newBuilder(eventMessage);
            for(final Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
                switch (entry.getKey()) {
                    case "event_name" -> builder.setField(eventNameField, entry.getValue().getAsString());
                    case "insert_id" -> builder.setField(insertIdField, entry.getValue().getAsString());
                    case "client_event_timestamp" -> {
                        final DynamicMessage timestamp = toTimestamp(entry.getValue());
                        if(timestamp != null) {
                            builder.setField(clientEventTimestampField, timestamp);
                        }
                    }
                    case "server_received_timestamp" -> {
                        final DynamicMessage timestamp = toTimestamp(entry.getValue());
                        if(timestamp != null) {
                            builder.setField(serverReceivedTimestampField, timestamp);
                        }
                    }
                    case "event_properties" -> {
                        if(!entry.getValue().isJsonObject()) {
                            throw new IllegalArgumentException("event_properties must be jsonObject");
                        }
                        final List<DynamicMessage> eventPropertiesMessages = createEventProperties(entry.getValue().getAsJsonObject());
                        if(!eventPropertiesMessages.isEmpty()) {
                            builder.setField(eventPropertiesField, eventPropertiesMessages);
                        }
                    }
                    case "user_properties" -> {
                        if(!entry.getValue().isJsonObject()) {
                            throw new IllegalArgumentException("user_properties must be jsonObject");
                        }
                        final List<DynamicMessage> userPropertiesMessages = createUserProperties(entry.getValue().getAsJsonObject());
                        if(!userPropertiesMessages.isEmpty()) {
                            builder.setField(userPropertiesField, userPropertiesMessages);
                        }
                    }
                    case "pre_login_temp_user_id" -> builder.setField(preLoginTempUserIdField, entry.getValue().getAsString());
                    case "session_id" -> builder.setField(sessionIdField, entry.getValue().getAsString());
                    case "country" -> builder.setField(countryField, entry.getValue().getAsString());
                    case "region" -> builder.setField(regionField, entry.getValue().getAsString());
                    case "city" -> builder.setField(cityField, entry.getValue().getAsString());
                    case "ip_address" -> builder.setField(ipAddressField, entry.getValue().getAsString());
                    case "device_id" -> builder.setField(deviceIdField, entry.getValue().getAsString());
                    case "app_version_id" -> builder.setField(appVersionIdField, entry.getValue().getAsString());
                }
            }

            if(!builder.hasField(eventNameField)) {
                if(eventName == null) {
                    throw new IllegalArgumentException("event_name is not found");
                }
                builder.setField(eventNameField, eventName);
            }

            if(!builder.hasField(clientEventTimestampField)) {
                builder.setField(clientEventTimestampField, toTimestamp(eventTime.getMillis() * 1000L));
            }

            if(!jsonObject.has("event_properties") && !jsonObject.has("user_properties")) {
                final List<DynamicMessage> eventPropertiesMessages = createEventProperties(jsonObject);
                if(!eventPropertiesMessages.isEmpty()) {
                    builder.setField(eventPropertiesField, eventPropertiesMessages);
                }
            }

            if(!builder.hasField(insertIdField)) {
                final int hash = builder.hashCode();
                builder.setField(insertIdField, Integer.toString(hash));
            }

            return builder.build();
        }

        private List<DynamicMessage> createEventProperties(final JsonObject jsonObject) {
            final List<DynamicMessage> eventProperties = new ArrayList<>();
            for(final Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
                if(PROPERTIES_EVENT.contains(entry.getKey()) || PROPERTIES_MESSAGE.contains(entry.getKey())) {
                    continue;
                }
                if(excludeFields != null && !excludeFields.isEmpty()) {
                    if(excludeFields.contains(entry.getKey())) {
                        continue;
                    }
                }

                final DynamicMessage propertyValueMessage = createPropertyValue(entry.getValue());
                if(propertyValueMessage != null) {
                    final DynamicMessage.Builder builder = DynamicMessage.newBuilder(eventPropertiesMessage);
                    builder.setField(eventPropertiesKeyField, entry.getKey());
                    builder.setField(eventPropertiesValueField, propertyValueMessage);
                    final DynamicMessage eventProperty = builder.build();
                    eventProperties.add(eventProperty);
                }
            }
            return eventProperties;
        }

        private List<DynamicMessage> createUserProperties(final JsonObject jsonObject) {
            final List<DynamicMessage> userProperties = new ArrayList<>();
            for(final Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
                if(PROPERTIES_EVENT.contains(entry.getKey()) || PROPERTIES_MESSAGE.contains(entry.getKey())) {
                    continue;
                }
                final DynamicMessage propertyValueMessage = createPropertyValue(entry.getValue());
                if(propertyValueMessage != null) {
                    final DynamicMessage.Builder builder = DynamicMessage.newBuilder(userPropertiesMessage);
                    builder.setField(userPropertiesKeyField, entry.getKey());
                    builder.setField(userPropertiesValueField, propertyValueMessage);
                    final DynamicMessage userProperty = builder.build();
                    userProperties.add(userProperty);
                }
            }
            return userProperties;
        }

        private DynamicMessage createPropertyValue(final JsonElement jsonElement) {
            if(jsonElement == null || jsonElement.isJsonNull()) {
                return null;
            }

            final DynamicMessage.Builder builder = DynamicMessage.newBuilder(propertyValueMessage);

            if(jsonElement.isJsonPrimitive()) {
                final JsonPrimitive jsonPrimitive = jsonElement.getAsJsonPrimitive();

                if(jsonPrimitive.isBoolean()) {
                    builder.setField(propertyValueBooleanField, jsonPrimitive.getAsBoolean());
                } else if(jsonPrimitive.isNumber()) {
                    switch (jsonPrimitive.getAsNumber()) {
                        case Short s -> builder.setField(propertyValueLongField, s.longValue());
                        case Integer i -> builder.setField(propertyValueLongField, i.longValue());
                        case Long l -> builder.setField(propertyValueLongField, l);
                        case Float f -> builder.setField(propertyValueDoubleField, f.doubleValue());
                        case Double d -> builder.setField(propertyValueDoubleField, d);
                        case BigDecimal bd -> builder.setField(propertyValueDoubleField, bd.doubleValue());
                        case LazilyParsedNumber p -> {
                            if(NumberUtils.isDigits(p.toString())) {
                                builder.setField(propertyValueLongField, p.longValue());
                            } else {
                                builder.setField(propertyValueDoubleField, p.doubleValue());
                            }
                        }
                        default -> throw new IllegalArgumentException("Illegal number: " + jsonPrimitive.getAsNumber().getClass());
                    }
                } else if(jsonPrimitive.isString()) {
                    final String text = jsonPrimitive.getAsString();
                    if(DateTimeUtil.isTimestamp(text)) {
                        final DynamicMessage timestamp = toTimestamp(text);
                        builder.setField(propertyValueTimestampField, timestamp);
                    } else {
                        builder.setField(propertyValueStringField, text);
                    }
                } else {
                    throw new IllegalArgumentException("Not supported property json primitive value: " + jsonPrimitive);
                }
            } else if(jsonElement.isJsonObject()) {
                final JsonObject jsonObject = jsonElement.getAsJsonObject();
                if(jsonObject.size() == 2 && jsonObject.has("seconds") && jsonObject.has("nanos")) {
                    final DynamicMessage timestamp = toTimestamp(jsonObject);
                    builder.setField(propertyValueTimestampField, timestamp);
                } else {
                    throw new IllegalArgumentException("Not supported property json object value: " + jsonObject);
                }
            } else {
                throw new IllegalArgumentException("Not supported property json element value: " + jsonElement);
            }

            return builder.build();
        }

    }

    public static Descriptors.Descriptor getMessage(
            final Descriptors.Descriptor messageDescriptor,
            final String fieldName) {

        final Descriptors.FieldDescriptor eventsField = getField(messageDescriptor, fieldName);
        return eventsField.getMessageType();
    }

    public static Descriptors.FieldDescriptor getField(
            final Descriptors.Descriptor messageDescriptor,
            final String fieldName) {

        return messageDescriptor.getFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("Not found field: " + fieldName + " in message: " + messageDescriptor.getFields()));
    }

}
