package com.mercari.solution.module.failure;

import com.mercari.solution.module.FailureSink;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.util.FailureUtil;
import com.mercari.solution.util.pipeline.OptionUtil;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


@FailureSink.Module(name="bigquery")
public class BigQueryFailureSink extends FailureSink {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryFailureSink.class);

    private static class Parameters implements Serializable {

        private String table;
        private BigQueryIO.Write.CreateDisposition createDisposition;
        private BigQueryIO.Write.Method method;
        private Boolean optimizedWrites;

        private void validate(String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.table == null) {
                errorMessages.add("parameters.table must not be null");
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(name, "failures", errorMessages);
            }
        }

        private void setDefaults(PInput input) {
            if(optimizedWrites == null) {
                optimizedWrites = true;
            }
            if(createDisposition == null) {
                createDisposition = BigQueryIO.Write.CreateDisposition.CREATE_NEVER;
            }
            if(method == null) {
                if(OptionUtil.isStreaming(input)) {
                    method = BigQueryIO.Write.Method.STORAGE_API_AT_LEAST_ONCE;
                } else {
                    method = BigQueryIO.Write.Method.FILE_LOADS;
                }
            }
        }
    }

    @Override
    public PDone expand(PCollection<BadRecord> input) {
        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(getName());
        parameters.setDefaults(input);

        BigQueryIO.Write<BadRecord> write = BigQueryIO
                .<BadRecord>write()
                .to(parameters.table)
                .withMethod(parameters.method)
                .optimizedWrites()
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(parameters.createDisposition)
                .withAvroFormatFunction(FailureUtil.createAvroConverter(jobName, getName()))
                .useAvroLogicalTypes();

        if(OptionUtil.isStreaming(input)) {

        } else {

        }

        final WriteResult writeResult = input.apply("WriteBigQuery", write);

        return PDone.in(input.getPipeline());
    }


}
