package com.mercari.solution.module;

import com.mercari.solution.config.Config;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MErrorHandler implements AutoCloseable, Serializable {

    private final ErrorHandler.BadRecordErrorHandler<?> errorHandler;

    private MErrorHandler(final ErrorHandler.BadRecordErrorHandler<?> errorHandler) {
        this.errorHandler = errorHandler;
    }

    public static MErrorHandler of(final ErrorHandler.BadRecordErrorHandler<?> errorHandler) {
        return new MErrorHandler(errorHandler);
    }

    public static MErrorHandler empty() {
        return new MErrorHandler(null);
    }

    public static MErrorHandler createPipelineErrorHandler(final Pipeline pipeline, final Config config) {
        if(config.getFailures().isEmpty()) {
            return empty();
        }
        if(Optional.ofNullable(config.getSystem().getFailure().getFailFast()).orElse(false)) {
            return empty();
        }
        if(!config.getSystem().getFailure().getUnion()) {
            return empty();
        }
        return of(registerErrorHandler(pipeline, config));
    }

    private static ErrorHandler.BadRecordErrorHandler<?> registerErrorHandler(
            final Pipeline pipeline,
            final Config config) {

        if(config.getFailures().isEmpty()) {
            return null;
        }
        final FailureSink.FailureSinks failureSinks = FailureSink.merge(getFailureSinks(config, pipeline.getOptions()));
        return pipeline.registerBadRecordErrorHandler(failureSinks);
    }

    private static List<FailureSink> getFailureSinks(Config config, PipelineOptions options) {
        return Optional
                .ofNullable(config.getFailures())
                .map(l -> l.stream().map(ll -> FailureSink.create(ll, "pipeline", options)).toList())
                .orElseGet(ArrayList::new);
    }

    @Override
    public void close() {
        if(errorHandler != null && !errorHandler.isClosed()) {
            errorHandler.close();
        }
    }

    public boolean isEmpty() {
        return this.errorHandler == null;
    }

    public void addError(final PCollection<BadRecord> errorCollection) {
        if(this.errorHandler != null && errorCollection != null) {
            this.errorHandler.addErrorCollection(errorCollection);
        }
    }

    public void apply(final PubsubIO.Write<?> write) {
        if(this.errorHandler != null && write != null) {
            write.withErrorHandler(errorHandler);
        }
    }

    public void apply(final PubsubIO.Read<?> read) {
        if(this.errorHandler != null && read != null) {
            read.withErrorHandler(errorHandler);
        }
    }

    public void apply(final KafkaIO.Read<?,?> read) {
        if(this.errorHandler != null && read != null) {
            read.withBadRecordErrorHandler(errorHandler);
        }
    }

    public void apply(final KafkaIO.Write<?,?> write) {
        if(this.errorHandler != null && write != null) {
            write.withBadRecordErrorHandler(errorHandler);
        }
    }

    public void apply(final BigtableIO.Write write) {
        if(this.errorHandler != null && write != null) {
            write.withErrorHandler(errorHandler);
        }
    }

    public void apply(final FileIO.Write<?,?> write) {
        if(this.errorHandler != null && write != null) {
            write.withBadRecordErrorHandler(errorHandler);
        }
    }

    public void apply(final BigQueryIO.Write<?> write) {
        if(this.errorHandler != null && write != null) {
            write.withErrorHandler(errorHandler);
        }
    }

    public void apply(final BigQueryIO.TypedRead<?> read) {
        if(this.errorHandler != null && read != null) {
            read.withErrorHandler(errorHandler);
        }
    }

    public void apply(final SqlTransform transform) {
        if(this.errorHandler != null && transform != null) {
            //transform.withErrorsTransformer(errorHandler);
        }
    }

}
