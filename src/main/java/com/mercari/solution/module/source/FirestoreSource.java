package com.mercari.solution.module.source;

import com.google.firestore.v1.*;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.*;
import com.mercari.solution.util.gcp.FirestoreUtil;
import com.mercari.solution.util.schema.converter.ElementToDocumentConverter;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Source.Module(name="firestore")
public class FirestoreSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(FirestoreSource.class);

    private static final Pattern PATTERN_CONDITION = Pattern.compile("(.+?)(s*>=s*|s*<=s*|s*=s*|s*>s*|s*<s*)(.+)");

    private static class Parameters implements Serializable {

        private String projectId;
        private String databaseId;
        private String collection;
        private String filter;
        private List<String> fields;
        private String orderField;
        private StructuredQuery.Direction orderDirection;
        private String parent;
        private Boolean allDescendants;
        private Boolean parallel;

        private Integer pageSize;
        private Long partitionCount;

        public void validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(this.collection == null) {
                errorMessages.add("parameters.collection must not be null");
            }
            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults(final PInput input) {
            if(this.projectId == null) {
                this.projectId = DataflowOptions.getProject(input.getPipeline().getOptions());
            }
            if(this.databaseId == null) {
                this.databaseId = FirestoreUtil.DEFAULT_DATABASE_NAME;
            }
            if(!FirestoreUtil.DEFAULT_DATABASE_NAME.equals(this.databaseId)) {
                input.getPipeline().getOptions().as(FirestoreOptions.class)
                        .setFirestoreDb(this.databaseId);
            }
            if(this.parallel == null) {
                this.parallel = false;
            }
            if(this.allDescendants == null) {
                this.allDescendants = false;
            }
            if(this.parent == null) {
                this.parent = "";
            } else if(!this.parent.startsWith("/")) {
                this.parent = "/" + this.parent;
            }
            if(this.fields == null) {
                this.fields = new ArrayList<>();
            }
            if(this.orderDirection == null) {
                this.orderDirection = StructuredQuery.Direction.ASCENDING;
            }
            if(this.partitionCount == null) {
                final int hintMaxNumWorkers = DataflowOptions.getMaxNumWorkers(input);
                this.partitionCount = hintMaxNumWorkers > 1 ? hintMaxNumWorkers - 1: 1L;
            }

        }

    }

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults(begin);

        final String parent = createParent(parameters);
        final PCollection<MElement> output;
        if(parameters.filter == null) {
            ListDocumentsRequest.Builder builder = ListDocumentsRequest
                    .newBuilder()
                    .setParent(parent);
            if(parameters.collection != null) {
                builder = builder.setCollectionId(parameters.collection);
            }
            if(!parameters.fields.isEmpty()) {
                DocumentMask.Builder maskBuilder = DocumentMask.newBuilder();
                for(final String field : parameters.fields) {
                    maskBuilder.addFieldPaths(field);
                }
                builder.setMask(maskBuilder.build());
            }

            final ListDocumentsRequest request = builder.build();
            output = begin
                    .apply("CreateListDocumentRequest", Create
                            .of(request)
                            .withCoder(SerializableCoder.of(ListDocumentsRequest.class)))
                    .apply("ListDocument", FirestoreIO.v1().read().listDocuments().build())
                    .apply("Convert", ParDo.of(new ConvertListResponseDoFn()));
        } else {
            final StructuredQuery structuredQuery = createQuery(getSchema(), parameters);
            final PCollection<RunQueryRequest> runQueryRequests;
            if(parameters.parallel) {
                PartitionQueryRequest request = PartitionQueryRequest.newBuilder()
                        .setParent(parent)
                        .setStructuredQuery(structuredQuery)
                        .setPartitionCount(parameters.partitionCount)
                        .build();

                runQueryRequests = begin
                        .apply("CreatePartitionQuery", Create
                                .of(request)
                                .withCoder(SerializableCoder.of(PartitionQueryRequest.class)))
                        .apply("SplitPartitionQuery", FirestoreIO.v1().read().partitionQuery().build());
            } else {
                final RunQueryRequest runQueryRequest = RunQueryRequest.newBuilder()
                        .setParent(parent)
                        .setStructuredQuery(structuredQuery)
                        .build();

                runQueryRequests = begin
                        .apply("CreateQuery", Create
                                .of(runQueryRequest)
                                .withCoder(SerializableCoder.of(RunQueryRequest.class)));
            }
            output = runQueryRequests
                    .apply("RunQuery", FirestoreIO.v1().read().runQuery().build())
                    .apply("FilterEmpty", Filter.by((RunQueryResponse::hasDocument)))
                    .apply("Convert", ParDo.of(new ConvertQueryResponseDoFn()));

        }
        return MCollectionTuple
                .of(output, getSchema());
    }

    private static class ConvertListResponseDoFn extends DoFn<Document, MElement> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Document document = c.element();
            final MElement output = MElement.of(document, c.timestamp());
            c.output(output);
        }

    }

    private static class ConvertQueryResponseDoFn extends DoFn<RunQueryResponse, MElement> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            final RunQueryResponse response = c.element();
            final Document document = response.getDocument();
            final MElement output = MElement.of(document, c.timestamp());//
            c.output(output);
        }

    }

    private static String createParent(Parameters parameters) {
        final String databaseRootName = FirestoreUtil
                .createDatabaseRootName(parameters.projectId, parameters.databaseId);
        return databaseRootName + "/documents" + parameters.parent;
    }

    private StructuredQuery createQuery(
            final Schema schema,
            final Parameters parameters) {

        StructuredQuery.CollectionSelector.Builder selectorBuilder = StructuredQuery.CollectionSelector
                .newBuilder()
                .setAllDescendants(parameters.allDescendants);
        if(parameters.collection != null) {
            selectorBuilder = selectorBuilder.setCollectionId(parameters.collection);
        }

        final StructuredQuery.Builder builder = StructuredQuery.newBuilder()
                .addFrom(selectorBuilder);

        if(!parameters.fields.isEmpty()) {
            final List<StructuredQuery.FieldReference> refers = new ArrayList<>();
            for(String field : parameters.fields) {
                refers.add(StructuredQuery.FieldReference.newBuilder()
                        .setFieldPath(field.trim())
                        .build());
            }
            builder.setSelect(StructuredQuery.Projection.newBuilder()
                    .addAllFields(refers)
                    .build());
        }

        if(parameters.filter != null) {
            try(final Scanner scanner = new Scanner(parameters.filter)
                    .useDelimiter("s*ands*|s*ors*|s*ANDs*|s*ORs*")) {

                final List<StructuredQuery.Filter> filters = new ArrayList<>();
                while(scanner.hasNext()) {
                    final String fragment = scanner.next();
                    final Matcher matcher = PATTERN_CONDITION.matcher(fragment);
                    if(matcher.find() && matcher.groupCount() > 2) {
                        final String field = matcher.group(1).trim();
                        final String op = matcher.group(2).trim();
                        final String strValue = matcher.group(3)
                                .trim()
                                .replaceAll("\"","")
                                .replaceAll("'","");

                        final Schema.FieldType fieldType = schema.getField(field).getFieldType();
                        final StructuredQuery.FieldFilter.Operator operator = convertOp(op);
                        final Value value = ElementToDocumentConverter.getValueFromString(fieldType, strValue);

                        final StructuredQuery.FieldFilter fieldFilter = StructuredQuery.FieldFilter.newBuilder()
                                .setField(StructuredQuery.FieldReference.newBuilder()
                                        .setFieldPath(field)
                                        .build())
                                .setOp(operator)
                                .setValue(value)
                                .build();
                        filters.add(StructuredQuery.Filter.newBuilder().setFieldFilter(fieldFilter).build());
                    } else {
                        throw new IllegalArgumentException("Failed to build query filter for: " + fragment);
                    }
                }

                if(filters.size() == 1) {
                    builder.setWhere(filters.get(0));
                } else if(filters.size() > 1) {
                    builder.setWhere(StructuredQuery.Filter.newBuilder()
                            .setCompositeFilter(StructuredQuery.CompositeFilter.newBuilder()
                                    .setOp(StructuredQuery.CompositeFilter.Operator.AND)
                                    .addAllFilters(filters)
                                    .build())
                            .build());
                }

            }
        }

        if(parameters.orderField != null) {
            final StructuredQuery.Order order = StructuredQuery.Order.newBuilder()
                    .setField(StructuredQuery.FieldReference.newBuilder().setFieldPath(parameters.orderField).build())
                    .setDirection(parameters.orderDirection)
                    .build();
            builder.addOrderBy(order);
        }

        return builder.build();
    }

    private static StructuredQuery.FieldFilter.Operator convertOp(final String op) {
        return switch (op.trim()) {
            case "=" -> StructuredQuery.FieldFilter.Operator.EQUAL;
            case ">" -> StructuredQuery.FieldFilter.Operator.GREATER_THAN;
            case "<" -> StructuredQuery.FieldFilter.Operator.LESS_THAN;
            case ">=" -> StructuredQuery.FieldFilter.Operator.GREATER_THAN_OR_EQUAL;
            case "<=" -> StructuredQuery.FieldFilter.Operator.LESS_THAN_OR_EQUAL;
            case "!=" -> StructuredQuery.FieldFilter.Operator.NOT_EQUAL;
            default -> throw new IllegalArgumentException("Not supported op type: " + op);
        };
    }

}
