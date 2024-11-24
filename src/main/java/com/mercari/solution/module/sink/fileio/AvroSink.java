package com.mercari.solution.module.sink.fileio;

import com.mercari.solution.module.DataType;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.sink.StorageSink;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import com.mercari.solution.util.schema.converter.ElementToAvroConverter;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;

public class AvroSink implements FileIO.Sink<KV<String, MElement>> {

    private final String jsonSchema;
    private final StorageSink.CodecName codecName;
    private final Map<String, String> metadata;
    private final boolean fitSchema;

    private transient @Nullable Schema schema;
    private transient @Nullable DataFileWriter<GenericRecord> writer;

    public static AvroSink of(
            final com.mercari.solution.module.Schema schema,
            final StorageSink.CodecName codecName,
            final boolean fitSchema) {

        return new AvroSink(schema.getAvroSchema().toString(), codecName, fitSchema);
    }

    AvroSink(
            final String jsonSchema,
            final StorageSink.CodecName codecName,
            final boolean fitSchema) {

        this.jsonSchema = jsonSchema;
        this.codecName = codecName;
        this.metadata = new HashMap<>();
        this.fitSchema = fitSchema;
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
        this.schema = new Schema.Parser().parse(jsonSchema);
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        final CodecFactory codecFactory = switch (this.codecName) {
            case BZIP2 -> CodecFactory.bzip2Codec();
            case SNAPPY -> CodecFactory.snappyCodec();
            case DEFLATE -> CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);
            case XZ -> CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL);
            case ZSTD -> CodecFactory.zstandardCodec(CodecFactory.DEFAULT_ZSTANDARD_LEVEL);
            default -> CodecFactory.nullCodec();
        };
        this.writer = new DataFileWriter<>(datumWriter).setCodec(codecFactory);
        metadata.forEach((key, value) -> writer.setMeta(key, value));
        writer.create(schema, Channels.newOutputStream(channel));
    }

    @Override
    public void write(KV<String, MElement> element) throws IOException {
        final MElement input = element.getValue();
        final GenericRecord record = ElementToAvroConverter.convert(schema, input);
        if(fitSchema && DataType.AVRO.equals(input.getType())) {
            final GenericRecord fitted = AvroSchemaUtil.toBuilder(schema, record).build();
            writer.append(fitted);
        } else {
            writer.append(record);
        }
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }
}