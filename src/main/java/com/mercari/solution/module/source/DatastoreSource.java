package com.mercari.solution.module.source;

import com.google.datastore.v1.Entity;
import com.mercari.solution.module.*;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.schema.converter.EntityToElementConverter;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;


@Source.Module(name="datastore")
public class DatastoreSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(DatastoreSource.class);

    private static class Parameters implements Serializable {

        private String projectId;
        private String gql;
        private String kind;
        private String namespace;
        private Integer numQuerySplits;
        private Boolean withKey;
        private Boolean emulator;

        private void validate() {

            // check required parameters filled
            final List<String> errorMessages = new ArrayList<>();
            if(gql == null) {
                errorMessages.add("parameters.gql must not be null");
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            if (withKey == null) {
                withKey = false;
            }
        }
    }

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();

        DatastoreV1.Read read = DatastoreIO.v1().read()
                .withProjectId(Optional
                        .ofNullable(parameters.projectId)
                        .orElseGet(OptionUtil::getDefaultProject))
                .withLiteralGqlQuery(parameters.gql);

        if(parameters.namespace != null) {
            read = read.withNamespace(parameters.namespace);
        }

        if(parameters.numQuerySplits != null) {
            read = read.withNumQuerySplits(parameters.numQuerySplits);
        }

        final Schema entitySchema = parameters.withKey ? EntityToElementConverter.addKeyToSchema(getSchema()) : getSchema();

        final PCollection<MElement> entities = begin
                .apply("ReadDatastore", read)
                .apply("Format", ParDo.of(new FormatDoFn(entitySchema, getTimestampAttribute())));

        return MCollectionTuple
                .of(entities, entitySchema.withType(DataType.ENTITY));
    }

    private static class FormatDoFn extends DoFn<Entity, MElement> {

        private final Schema schema;
        private final String timestampAttribute;

        FormatDoFn(Schema schema, String timestampAttribute) {
            this.schema = schema;
            this.timestampAttribute = timestampAttribute;
        }

        @Setup
        public void setup() {
            this.schema.setup();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement element = MElement.of(c.element(), c.timestamp());
            if(timestampAttribute == null) {
                c.output(element);
            } else {
                final Instant timestamp = element.getAsJodaInstant(timestampAttribute);
                c.outputWithTimestamp(element, timestamp);
            }
        }

    }

}
