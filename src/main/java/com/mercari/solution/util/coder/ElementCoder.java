package com.mercari.solution.util.coder;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.spanner.Struct;
import com.google.datastore.v1.Entity;
import com.google.firestore.v1.Document;
import com.mercari.solution.module.DataType;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.extensions.protobuf.DynamicProtoCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdAndOrderingKeyCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ElementCoder extends StructuredCoder<MElement> {

    private final List<Coder<?>> coders;

    public ElementCoder(List<Coder<?>> coders) {
        if(coders.isEmpty()) {
            throw new IllegalStateException("ElementCoder coders must not empty");
        }
        this.coders = coders;
    }

    public static ElementCoder of(Schema schema) {
        final Coder<?> innerCoder = getInnerCoder(schema);
        return new ElementCoder(List.of(innerCoder));
    }

    public static ElementCoder of(List<Schema> schemas) {
        final List<Coder<?>> innerCoders = new ArrayList<>();
        for(final Schema schema : schemas) {
            final Coder<?> innerCoder = getInnerCoder(schema);
            innerCoders.add(innerCoder);
        }
        return new ElementCoder(innerCoders);
    }


    private static Coder<?> getInnerCoder(Schema schema) {
        return switch (schema.getType()) {
            case ELEMENT -> UnionMapCoder.mapCoder();
            case ROW -> RowCoder.of(schema.getRow().getSchema());
            case AVRO -> AvroGenericCoder.of(schema.getAvro().getSchema());
            case PROTO -> DynamicProtoCoder.of(schema.getProtobuf().getDescriptor());
            case STRUCT -> SerializableCoder.of(Struct.class);
            case DOCUMENT -> SerializableCoder.of(Document.class);
            case ENTITY -> SerializableCoder.of(Entity.class);
            case MESSAGE -> PubsubMessageWithAttributesAndMessageIdAndOrderingKeyCoder.of();
            case SPANNER_DATACHANGERECORD -> AvroGenericCoder.of(DataChangeRecord.class);
            case BIGTABLE_DATACHANGERECORD -> SerializableCoder.of(ChangeStreamMutation.class);
            default -> throw new IllegalStateException("Not supported schema: " + schema.getType());
        };
    }

    private int getIndex(MElement value) {
        int index = value.getIndex();
        if(index < 0) {
            throw new IllegalStateException("MElement index must over zero. actual index is: " + index);
        }
        if(index >= coders.size()) {
            throw new IllegalStateException("MElement index must over coders size. actual index is: " + index + ", coders size is: " + coders.size());
        }
        return index;
    }

    public List<? extends Coder<?>> getCoders() {
        return coders;
    }

    @Override
    public void encode(MElement value, OutputStream outStream) throws IOException {
        encode(value, outStream, Context.NESTED);
    }

    @Override
    public void encode(MElement value, OutputStream outStream, Context context) throws IOException {
        final int index = getIndex(value);
        VarInt.encode(index, outStream);
        VarInt.encode(value.getType().getId(), outStream);
        VarInt.encode(value.getEpochMillis(), outStream);

        final Coder<Object> coder = (Coder<Object>) coders.get(index);
        validate(coder, value.getType());
        coder.encode(value.getValue(), outStream, context);
    }

    @Override
    public MElement decode(InputStream inStream) throws IOException {
        return decode(inStream, Context.NESTED);
    }

    @Override
    public MElement decode(InputStream inStream, Context context) throws  IOException {
        final int index = VarInt.decodeInt(inStream);
        final int dataTypeId = VarInt.decodeInt(inStream);
        final DataType dataType = DataType.of(dataTypeId);
        final long epochMillis = VarInt.decodeLong(inStream);

        final Coder<?> coder = coders.get(index);
        validate(coder, dataType);

        final Object value = coder.decode(inStream, context);
        return MElement.of(index, dataType, value, epochMillis);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override
    public List<? extends Coder<?>> getComponents() {
        return coders;
    }

    @Override
    public void registerByteSizeObserver(MElement value, ElementByteSizeObserver observer) throws Exception {
        final int index = getIndex(value);
        observer.update(VarInt.getLength(index));
        final Coder<Object> coder = (Coder<Object>) coders.get(index);
        coder.registerByteSizeObserver(value.getValue(), observer);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        verifyDeterministic(this, "ElementCoder is deterministic if all coders are deterministic");
    }

    private void validate(final Coder<?> coder, final DataType dataType) {
        final boolean ok = switch (coder) {
            case MapCoder<?,?> c when !dataType.equals(DataType.ELEMENT) -> throw new IllegalStateException("Illegal data type: " + dataType + " for MapCoder");
            case RowCoder c when !dataType.equals(DataType.ROW) -> throw new IllegalStateException("Illegal data type: " + dataType + " for RowCoder");
            case AvroGenericCoder c when !dataType.equals(DataType.AVRO) -> throw new IllegalStateException("Illegal data type: " + dataType + " for AvroCoder");
            case DynamicProtoCoder c when !dataType.equals(DataType.PROTO) -> throw new IllegalStateException("Illegal data type: " + dataType + " for ProtoCoder");
            case PubsubMessageWithAttributesAndMessageIdAndOrderingKeyCoder c when !dataType.equals(DataType.MESSAGE) -> throw  new IllegalStateException("Illegal data type: " + dataType + " for PubSubMessageCoder");
            default -> true;
        };
    }

}
