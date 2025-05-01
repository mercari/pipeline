package com.mercari.solution.util.pipeline;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.sink.StorageSink;
import com.mercari.solution.module.sink.fileio.AvroSink;
import com.mercari.solution.util.TemplateFileNaming;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.schema.converter.ElementToAvroConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class Gather {

    public static WriteFile of(String directory, String filename, Schema schema) {
        return new WriteFile(directory, filename, schema);
    }

    public static class WriteFile extends PTransform<PCollection<MElement>, PDone> {

        private final String directory;
        private final String filename;
        private final Schema schema;

        WriteFile(String directory, String filename, Schema schema) {
            this.directory = directory;
            this.filename = filename;
            this.schema = schema;
        }

        @Override
        public PDone expand(PCollection<MElement> input) {

            schema.setup();

            final WriteFilesResult<String> d = input
                    .apply("WithKey", WithKeys.of(directory))
                    .setCoder(KvCoder.of(StringUtf8Coder.of(), ElementCoder.of(schema)))
                    .apply("Write", FileIO.<String, KV<String,MElement>>writeDynamic()
                            .to(directory)
                            .by(b -> "")
                            .via(AvroSink.of(schema, StorageSink.CodecName.UNCOMPRESSED, false))
                            .withNumShards(1)
                            .withNoSpilling()
                            .withNaming(key -> TemplateFileNaming.of(filename, key))
                            .withDestinationCoder(StringUtf8Coder.of()));

            d.getPerDestinationOutputFilenames()
                    .apply("FileNames", ParDo.of(new PrintDoFn()));

            return PDone.in(input.getPipeline());
        }

    }

    private static class ConvertDoFn extends DoFn<MElement, GenericRecord> {

        private final Schema schema;

        ConvertDoFn(final Schema schema) {
            this.schema = schema;
        }

        @Setup
        public void setup() {
            schema.setup();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final GenericRecord record = ElementToAvroConverter.convert(schema, c.element());
            c.output(record);
        }
    }

    private static class PrintDoFn extends DoFn<KV<String,String>, Void> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("print: " + c.element());
        }

    }

    private static class ObjectNameDoFn extends DoFn<MElement, KV<String, MElement>> {

        private final String path;
        public ObjectNameDoFn(final String path) {

            this.path = path;
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final MElement element = c.element();
            c.output(KV.of(this.path, element));
        }

    }


}
