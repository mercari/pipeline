package com.mercari.solution.module;

import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;
import com.mercari.solution.util.coder.ElementCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

import java.util.*;
import java.util.stream.Collectors;

public class MCollectionTuple implements PInput, POutput {

    private static final String TAG_DEFAULT = "";
    private static final String TAG_FAILURES = "failures";

    private final Pipeline pipeline;
    private final Map<String, PCollection<MElement>> pcollectionMap;
    private final Map<String, Schema> schemaMap;


    private MCollectionTuple(Pipeline pipeline) {
        this(pipeline, new LinkedHashMap<>(), new LinkedHashMap<>());
    }

    private MCollectionTuple(Pipeline pipeline, Map<String, PCollection<MElement>> pcollectionMap, Map<String, Schema> schemaMap) {
        this.pipeline = pipeline;
        this.pcollectionMap = Collections.unmodifiableMap(pcollectionMap);
        this.schemaMap = schemaMap;
    }


    public static MCollectionTuple empty(Pipeline pipeline) {
        return new MCollectionTuple(pipeline);
    }

    public static MCollectionTuple done(PDone done) {
        return new MCollectionTuple(done.getPipeline());
    }

    public static MCollectionTuple of(PCollection<MElement> pc, Schema schema) {
        return empty(pc.getPipeline())
                .and(TAG_DEFAULT, pc.setCoder(ElementCoder.of(schema)), schema);
    }

    public static MCollectionTuple of(String tag, PCollection<MElement> pc, Schema schema) {
        return empty(pc.getPipeline())
                .and(tag, pc.setCoder(ElementCoder.of(schema)), schema);
    }

    public MCollectionTuple and(String tag, PCollection<MElement> pc, Schema schema) {
        if (pc.getPipeline() != pipeline) {
            throw new IllegalArgumentException("PCollections come from different Pipelines");
        }

        return new MCollectionTuple(
                pipeline,
                new ImmutableMap.Builder<String, PCollection<MElement>>()
                        .putAll(pcollectionMap)
                        .put(tag, pc.setCoder(ElementCoder.of(schema)))
                        .build(),
                new ImmutableMap.Builder<String, Schema>()
                        .putAll(schemaMap)
                        .put(tag, schema)
                        .build());
    }

    public MCollectionTuple failure(PCollection<MElement> failure) {
        if(failure == null) {
            return this;
        }

        if (failure.getPipeline() != pipeline) {
            throw new IllegalArgumentException("PCollections come from different Pipelines");
        }

        return new MCollectionTuple(
                pipeline,
                new ImmutableMap.Builder<String, PCollection<MElement>>()
                        .putAll(pcollectionMap)
                        .put(TAG_FAILURES, failure.setCoder(ElementCoder.of(MFailure.schema())))
                        .build(),
                new ImmutableMap.Builder<String, Schema>()
                        .putAll(schemaMap)
                        .put(TAG_FAILURES, MFailure.schema())
                        .build());
    }

    public MCollectionTuple failure(PCollectionList<MElement> failures) {
        return failure(failures.apply(Flatten.pCollections()));
    }

    public int size() {
        return this.pcollectionMap.size();
    }

    public boolean has(String tag) {
        return pcollectionMap.containsKey(tag);
    }

    public PCollection<MElement> get(String tag) {
        PCollection<MElement> pcollection = pcollectionMap.get(tag);
        if (pcollection == null) {
            throw new IllegalArgumentException("Tag[" + tag + "] not found in this MCollection tuple");
        }
        return pcollection;
    }

    public PCollection<MElement> getSinglePCollection() {
        Preconditions.checkState(
                pcollectionMap.size() == 1,
                "Expected exactly one output PCollection<MElement>, but found %s. "
                        + "Please try retrieving a specified output using get(<tag>) instead.",
                pcollectionMap.size());
        return get(pcollectionMap.entrySet().iterator().next().getKey());
    }

    public Map<String, PCollection<MElement>> getAll() {
        return pcollectionMap;
    }

    public List<String> getAllInputs() {
        return new ArrayList<>(schemaMap.keySet());
    }

    public Schema getSchema(String tag) {
        return schemaMap.get(tag);
    }

    public List<Schema> getAllSchema() {
        return new ArrayList<>(schemaMap.values());
    }

    public Map<String, Schema> getAllSchemaAsMap() {
        return new HashMap<>(schemaMap);
    }

    public Schema getSingleSchema() {
        Preconditions.checkState(
                pcollectionMap.size() == 1,
                "Expected exactly one output PCollection<MElement>, but found %s. "
                        + "Please try retrieving a specified output using get(<tag>) instead.",
                pcollectionMap.size());
        return getSchema(pcollectionMap.entrySet().iterator().next().getKey());
    }

    public Map<String, MCollectionTuple> asMap() {
        final Map<String, MCollectionTuple> flatten = new LinkedHashMap<>();
        for(final Map.Entry<String, PCollection<MElement>> entry : pcollectionMap.entrySet()) {
            final MCollectionTuple collection = MCollectionTuple
                    .of(entry.getKey(), entry.getValue(), schemaMap.get(entry.getKey()));
            flatten.put(entry.getKey(), collection);
        }
        return flatten;
    }

    public Map<String, MCollection> asCollectionMap() {
        final Map<String, MCollection> flatten = new LinkedHashMap<>();
        for(final Map.Entry<String, PCollection<MElement>> entry : pcollectionMap.entrySet()) {
            final MCollection collection = MCollection
                    .of(entry.getKey(), entry.getValue(), schemaMap.get(entry.getKey()));
            flatten.put(entry.getKey(), collection);
        }
        return flatten;
    }

    public <OutputT extends POutput> OutputT apply(PTransform<? super MCollectionTuple, OutputT> t) {
        return Pipeline.applyTransform(this, t);
    }

    public <OutputT extends POutput> OutputT apply(String name, PTransform<? super MCollectionTuple, OutputT> t) {
        return Pipeline.applyTransform(name, this, t);
    }

    public MCollectionTuple apply(Transform t) {
        return Pipeline.applyTransform(this, t);
    }

    public MCollectionTuple apply(Sink t) {
        return Pipeline.applyTransform(this, t);
    }

    public MCollectionTuple apply(String name, Transform t) {
        return Pipeline.applyTransform(name, this, t);
    }

    public MCollectionTuple apply(String name, Sink t) {
        return Pipeline.applyTransform(name, this, t);
    }

    @Override
    public Pipeline getPipeline() {
        return pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
        ImmutableMap.Builder<TupleTag<?>, PValue> builder = ImmutableMap.builder();
        pcollectionMap.forEach((tag, value) -> builder.put(new TupleTag<MElement>(tag), value));
        return builder.build();
    }

    @Override
    public void finishSpecifyingOutput(String transformName, PInput input, PTransform<?, ?> transform) {
        // All component PCollections will already have been finished. Update their names if
        // appropriate.
        for (Map.Entry<String, PCollection<MElement>> entry : pcollectionMap.entrySet()) {
            String tag = entry.getKey();
            PCollection<MElement> pc = entry.getValue();
            if (pc.getName().equals(defaultName(transformName))) {
                pc.setName(String.format("%s.%s", transformName, tag));
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MCollectionTuple)) {
            return false;
        }
        MCollectionTuple that = (MCollectionTuple) other;
        return this.pipeline.equals(that.pipeline) && this.pcollectionMap.equals(that.pcollectionMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.pipeline, this.pcollectionMap);
    }

    @Override
    public String toString() {
        final JsonObject jsonObject = new JsonObject();
        {
            JsonObject schemasObject = new JsonObject();
            for(final Map.Entry<String, Schema> entry : schemaMap.entrySet()) {
                schemasObject.addProperty(entry.getKey(), Optional.ofNullable(entry.getValue()).map(Schema::toString).orElse(""));
            }
            jsonObject.add("schemas", schemasObject);
        }
        return jsonObject.toString();
    }

    public MCollectionTuple withSource(String name) {
        return new MCollectionTuple(
                pipeline,
                new ImmutableMap.Builder<String, PCollection<MElement>>()
                        .putAll(pcollectionMap.entrySet()
                                .stream()
                                .collect(Collectors.toMap(
                                        e -> e.getKey().isEmpty() ? name : name + "." + e.getKey(),
                                        Map.Entry::getValue)))
                        .build(),
                new ImmutableMap.Builder<String, Schema>()
                        .putAll(schemaMap.entrySet()
                                .stream()
                                .collect(Collectors.toMap(
                                        e -> e.getKey().isEmpty() ? name : name + "." + e.getKey(),
                                        Map.Entry::getValue)))
                        .build());
    }

    static String defaultName(String transformName) {
        return String.format("%s.%s", transformName, "out");
    }

    public static MCollectionTuple mergeTuple(final List<MCollectionTuple> collections) {
        if(collections.isEmpty()) {
            throw new IllegalArgumentException("MCollections to merge is empty!");
        }
        final Map<String, PCollection<MElement>> collectionMap = new LinkedHashMap<>();
        final Map<String, Schema> schemaMap = new LinkedHashMap<>();
        for(final MCollectionTuple collection : collections) {
            collectionMap.putAll(collection.pcollectionMap);
            schemaMap.putAll(collection.schemaMap);
        }
        return new MCollectionTuple(
                collections.get(0).getPipeline(),
                new ImmutableMap.Builder<String, PCollection<MElement>>()
                        .putAll(collectionMap)
                        .build(),
                new ImmutableMap.Builder<String, Schema>()
                        .putAll(schemaMap)
                        .build());
    }

    public static MCollectionTuple mergeCollection(final List<MCollection> collections) {
        if(collections.isEmpty()) {
            throw new IllegalArgumentException("MCollections to merge is empty!");
        }
        final Map<String, PCollection<MElement>> collectionMap = new LinkedHashMap<>();
        final Map<String, Schema> schemaMap = new LinkedHashMap<>();
        for(final MCollection collection : collections) {
            collectionMap.put(collection.getName(), collection.getCollection());
            schemaMap.put(collection.getName(), collection.getSchema());
        }
        return new MCollectionTuple(
                collections.get(0).getPipeline(),
                new ImmutableMap.Builder<String, PCollection<MElement>>()
                        .putAll(collectionMap)
                        .build(),
                new ImmutableMap.Builder<String, Schema>()
                        .putAll(schemaMap)
                        .build());
    }

}
