package com.mercari.solution.module.sink.fileio;

import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.schema.converter.ElementToJsonConverter;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;


public class JsonSink implements FileIO.Sink<KV<String, MElement>> {

    private final Schema schema;
    private final boolean fitSchema;
    private final Boolean bom;

    private transient @Nullable PrintWriter writer;

    public static JsonSink of(
            final Schema schema,
            final boolean fitSchema,
            final boolean bom) {

        return new JsonSink(schema, fitSchema, bom);
    }

    JsonSink(
            final Schema schema,
            final boolean fitSchema,
            final boolean bom) {

        this.schema = schema;
        this.fitSchema = fitSchema;
        this.bom = bom;
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
        final OutputStream os = Channels.newOutputStream(channel);
        if(this.bom) {
            os.write(0xef);
            os.write(0xbb);
            os.write(0xbf);
        }
        this.writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8)));
    }

    @Override
    public void write(KV<String, MElement> element) throws IOException {
        final MElement input = element.getValue();
        final JsonObject json = ElementToJsonConverter.convert(schema, input);
        writer.println(json.toString());
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }
}
