package com.mercari.solution.util.schema.converter;

import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.joda.time.Instant;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class PubSubToAvroConverter {

    public static GenericRecord convertMessage(final Schema schema,
                                               final PubsubMessage message,
                                               final Instant timestamp) {

        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        if(message.getPayload() == null) {
            builder.set("payload", null);
        } else {
            builder.set("payload", ByteBuffer.wrap(message.getPayload()));
        }
        builder.set("messageId", message.getMessageId());
        if(message.getAttributeMap() == null) {
            builder.set("attributes", new ArrayList<>());
        } else {
            builder.set("attributes", message.getAttributeMap());
        }
        if(timestamp != null) {
            builder.set("timestamp", timestamp.getMillis() * 1000L);
        }
        return builder.build();
    }

    public static Schema createMessageSchema() {
        final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
        schemaFields.name("payload")
                .type(AvroSchemaUtil.NULLABLE_BYTES)
                .noDefault();
        schemaFields.name("messageId")
                .type(AvroSchemaUtil.NULLABLE_STRING)
                .noDefault();
        schemaFields.name("attributes")
                .type(AvroSchemaUtil.NULLABLE_MAP_STRING)
                .noDefault();
        schemaFields.name("timestamp")
                .type(AvroSchemaUtil.NULLABLE_LOGICAL_TIMESTAMP_MICRO_TYPE)
                .noDefault();
        return schemaFields.endRecord();
    }

}
