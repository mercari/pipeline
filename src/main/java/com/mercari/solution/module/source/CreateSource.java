package com.mercari.solution.module.source;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.pipeline.Select;
import com.mercari.solution.util.pipeline.select.SelectFunction;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

@Source.Module(name="create")
public class CreateSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(CreateSource.class);

    private static class Parameters implements Serializable {

        private String type;
        private JsonArray elements;
        private String from;
        private String to;
        private Integer interval;
        private DateTimeUtil.TimeUnit intervalUnit;

        private Long rate;
        private DateTimeUtil.TimeUnit rateUnit;

        private JsonElement filter;
        private JsonArray select;
        private String flattenField;

        private Integer splitSize;

        private Schema.Type elementType;


        private void validate(final String name, final Schema schema) {

            final List<String> errorMessages = new ArrayList<>();
            if((elements == null || !elements.isJsonArray()) && from == null) {
                errorMessages.add("create source module[" + name + "] requires either elements or from parameter");
            } else {
                if(type == null) {
                    errorMessages.add("create source module[" + name + "] requires type parameter in ['int','long','date','time','timestamp','string','element']");
                } else {
                    switch (Schema.Type.valueOf(type)) {
                        case element -> {
                            if(schema == null) {
                                errorMessages.add("create source module[" + name + "].schema must not be null if type is element");
                            }
                        }
                    }
                }
            }

            if(rate != null) {
                if(rate < 0) {
                    errorMessages.add("create source module[" + name + "].rate parameter must be over zero");
                }
            } else if((elements == null || elements.isEmpty()) && to == null) {
                errorMessages.add("create source module[" + name + "].to parameter is required when rate is not set");
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        private void setDefaults(final PInput input) {
            this.elementType = Schema.Type.of(type);
            if(this.elements == null) {
                this.elements = new JsonArray();
            }
            if(this.elements.isEmpty()) {
                if(this.interval == null) {
                    this.interval = 1;
                }
                if(this.intervalUnit == null && this.type != null) {
                    this.intervalUnit = switch (this.elementType) {
                        case date -> DateTimeUtil.TimeUnit.day;
                        case time, timestamp -> DateTimeUtil.TimeUnit.minute;
                        default -> null;
                    };
                }
            }

            if(this.from != null) {
                final Map<String, Object> values = new HashMap<>();
                TemplateUtil.setFunctions(values);
                this.from = TemplateUtil.executeStrictTemplate(from, values);
            }
            if(this.to != null) {
                final Map<String, Object> values = new HashMap<>();
                TemplateUtil.setFunctions(values);
                this.to = TemplateUtil.executeStrictTemplate(to, values);
            }

            if(this.rate == null) {
                this.rate = 0L;
            } else {
                if(this.rateUnit == null) {
                    this.rateUnit = DateTimeUtil.TimeUnit.minute;
                }
            }

            if(this.splitSize == null) {
                this.splitSize = 10;
            }

        }

    }

    @Override
    public MCollectionTuple expand(PBegin begin) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate(getName(), getSchema());
        parameters.setDefaults(begin);

        final DataType outputType = Optional
                .ofNullable(getOutputType())
                .orElse(DataType.ELEMENT);

        final Schema.FieldType elementFieldType = switch (parameters.elementType) {
            case element -> Schema.FieldType.element(getSchema());
            default -> Schema.FieldType.type(parameters.elementType);
        };
        final Schema elementSchema = createElementSchema(elementFieldType);
        final Schema outputSchema;
        final List<SelectFunction> selectFunctions;
        if(parameters.select == null || parameters.select.isEmpty()) {
            selectFunctions = new ArrayList<>();
            outputSchema = elementSchema;
        } else {
            selectFunctions = SelectFunction.of(parameters.select, elementSchema.getFields());
            outputSchema = SelectFunction.createSchema(selectFunctions, parameters.flattenField);
        }

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<MElement> failuresTag = new TupleTag<>() {};

        final long elementSize = calculateElementSize(parameters);
        final PCollectionTuple outputs;
        if(parameters.rate > 0) {
            final DoFn<Long, MElement> elementDoFn = new SequenceElementDoFn(
                    getJobName(), getName(),
                    elementFieldType, parameters, getTimestampAttribute(), getLoggings(),
                    outputSchema, parameters.filter, selectFunctions, parameters.flattenField, outputType,
                    getFailFast(), failuresTag);
            GenerateSequence generateSequence = GenerateSequence
                    .from(0)
                    .withRate(parameters.rate, DateTimeUtil.getDuration(parameters.rateUnit, 1L));
            if(elementSize > 0) {
                generateSequence = generateSequence.to(elementSize);
            }
            outputs = begin
                    .apply("GenerateSequence", generateSequence)
                    .apply("Element", ParDo
                            .of(elementDoFn)
                            .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
        } else {
            final DoFn<Long, MElement> elementDoFn;
            if(elementSize > 0) {
                elementDoFn = new BatchElementDoFn(
                        getJobName(), getName(),
                        elementFieldType, parameters, getTimestampAttribute(), elementSize, getLoggings(),
                        outputSchema, parameters.filter, selectFunctions, parameters.flattenField, outputType,
                        getFailFast(), failuresTag);
            } else {
                elementDoFn = new BatchElementDoFn(
                        getJobName(), getName(),
                        elementFieldType, parameters, getTimestampAttribute(), elementSize, getLoggings(),
                        outputSchema, parameters.filter, selectFunctions, parameters.flattenField, outputType,
                        getFailFast(), failuresTag);
            }
            final String nameSuffix = getTimestampAttribute() != null ? "WithTimestamp" : "";
            outputs = begin
                    .apply("Seed", Create.of(0L).withCoder(VarLongCoder.of()))
                    .apply("Element" + nameSuffix, ParDo
                            .of(elementDoFn)
                            .withOutputTags(outputTag, TupleTagList.of(failuresTag)));
        }

        outputSchema.withType(outputType);
        return MCollectionTuple
                .of(outputs.get(outputTag), outputSchema)
                .failure(outputs.get(failuresTag));
    }

    @DoFn.BoundedPerElement
    public static class BatchElementDoFn extends DoFn<Long, MElement> {

        private final String jobName;
        private final String moduleName;

        private final List<String> elements;
        private final String from;
        private final Integer interval;
        private final DateTimeUtil.TimeUnit intervalUnit;
        private final String timestampAttribute;
        private final Schema.FieldType elementFieldType;

        private final long size;
        private final boolean enableSplit;
        private final Integer splitSize;

        private final Select select;

        private final Map<String, Logging> logging;

        private final boolean failFast;
        private final TupleTag<MElement> failuresTag;

        BatchElementDoFn(
                final String jobName,
                final String moduleName,
                //
                final Schema.FieldType elementFieldType,
                final Parameters parameters,
                final String timestampAttribute,
                final long elementSize,
                final List<Logging> logging,
                //
                final Schema outputSchema,
                final JsonElement filterJson,
                final List<SelectFunction> selectFunctions,
                final String flattenField,
                final DataType outputType,
                //
                final boolean failFast,
                final TupleTag<MElement> failuresTag) {

            this.jobName = jobName;
            this.moduleName = moduleName;

            this.elementFieldType = elementFieldType;
            this.elements = new ArrayList<>();
            if(parameters.elements.isJsonArray()) {
                for(final JsonElement element : parameters.elements) {
                    this.elements.add(element.toString());
                }
            }
            this.from = parameters.from;
            this.interval = parameters.interval;
            this.intervalUnit = parameters.intervalUnit;
            this.timestampAttribute = timestampAttribute;

            this.size = elementSize;
            this.splitSize = parameters.splitSize;
            this.enableSplit = true;

            this.select = Select.of(outputSchema, filterJson, selectFunctions, flattenField, outputType);

            this.logging = Logging.map(logging);

            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            select.setup();
        }

        @ProcessElement
        public void processElement(
                final ProcessContext c,
                final RestrictionTracker<OffsetRange, Long> tracker) {

            final OffsetRange offsetRange = tracker.currentRestriction();
            if(offsetRange == null) {
                return;
            }
            long position = offsetRange.getFrom();
            while (tracker.tryClaim(position)) {
                try {
                    final Object elementValue;
                    if (!elements.isEmpty()) {
                        elementValue = createElements(elementFieldType, elements, position);
                    } else {
                        elementValue = createElement(elementFieldType, from, interval, intervalUnit, position);
                    }
                    Map<String, Object> map = createElement(elementFieldType, elementValue, position);

                    if (select.useSelect()) {
                        map = select.select(map, c.timestamp());
                    }

                    if (timestampAttribute != null) {
                        final Object timestampValue = map.get(timestampAttribute);
                        final org.joda.time.Instant eventTime = DateTimeUtil.toJodaInstant(timestampValue);
                        MElement output = MElement.builder(map).withEventTime(eventTime).build();
                        output = select.convert(output);
                        c.outputWithTimestamp(output, eventTime);
                        Logging.log(LOG, logging, "output", output);
                    } else {
                        MElement output = MElement.of(map, c.timestamp());
                        output = select.convert(output);
                        c.output(output);
                        Logging.log(LOG, logging, "output", output);
                    }
                    position++;
                } catch (final Throwable e) {
                    //errorCounter.inc();
                    final MFailure failure = MFailure
                            .of(jobName, moduleName, "position: " + position, e, c.timestamp());
                    final String errorMessage = String.format("Failed to execute create batch element for position: %d, error: %s", position, e.getMessage());
                    LOG.error(errorMessage);
                    if(failFast) {
                        throw new RuntimeException(errorMessage, e);
                    }
                    c.output(failuresTag, failure.toElement(c.timestamp()));
                }
            }

        }

        @GetInitialRestriction
        public OffsetRange getInitialRestriction()  {
            final OffsetRange initialOffsetRange = new OffsetRange(0L, size);
            LOG.info("Initial restriction: {}", initialOffsetRange);
            return initialOffsetRange;
        }

        @GetRestrictionCoder
        public Coder<OffsetRange> getRestrictionCoder() {
            return OffsetRange.Coder.of();
        }

        @SplitRestriction
        public void splitRestriction(
                @Restriction OffsetRange restriction,
                OutputReceiver<OffsetRange> splitReceiver) {

            if(enableSplit) {
                long size = (restriction.getTo() - restriction.getFrom()) / this.splitSize;
                if(size == 0) {
                    LOG.info("Not split restriction because size is zero");
                    splitReceiver.output(new OffsetRange(restriction.getFrom(), restriction.getTo()));
                    return;
                }
                long start = restriction.getFrom();
                for(int i=1; i<this.splitSize; i++) {
                    long end = i * size;
                    final OffsetRange childRestriction = new OffsetRange(start, end);
                    splitReceiver.output(childRestriction);
                    start = end;
                    LOG.info("create split restriction[{}]: {} for batch mode", i - 1, childRestriction);
                }
                final OffsetRange lastChildRestriction = new OffsetRange(start, restriction.getTo());
                splitReceiver.output(lastChildRestriction);
                LOG.info("create split restriction[{}]: {} for batch mode", this.splitSize - 1, lastChildRestriction);
            } else {
                LOG.info("Not split restriction: {} for batch mode", restriction);
                splitReceiver.output(restriction);
            }
        }

        @GetSize
        public double getSize(@Restriction OffsetRange restriction) throws Exception {
            final double size = restriction.getTo() - restriction.getFrom();
            LOG.info("SDF get size: {}", size);
            return size;
        }

    }

    private static class SequenceElementDoFn extends DoFn<Long, MElement> {

        private final String jobName;
        private final String moduleName;

        private final Schema.FieldType elementFieldType;
        private final List<String> elements;
        private final String from;
        private final Integer interval;
        private final DateTimeUtil.TimeUnit intervalUnit;
        private final String timestampAttribute;

        private final Select select;
        private final Map<String, Logging> logging;

        private final boolean failFast;
        private final TupleTag<MElement> failuresTag;

        SequenceElementDoFn(
                final String jobName,
                final String moduleName,
                //
                final Schema.FieldType elementFieldType,
                final Parameters parameters,
                final String timestampAttribute,
                final List<Logging> logging,
                //
                final Schema outputSchema,
                final JsonElement filterJson,
                final List<SelectFunction> selectFunctions,
                final String flattenField,
                final DataType outputType,
                //
                final boolean failFast,
                final TupleTag<MElement> failuresTag) {

            this.jobName = jobName;
            this.moduleName = moduleName;

            this.elementFieldType = elementFieldType;
            this.elements = new ArrayList<>();
            for(final JsonElement element : parameters.elements) {
                this.elements.add(element.toString());
            }
            this.from = parameters.from;
            this.interval = parameters.interval;
            this.intervalUnit = parameters.intervalUnit;
            this.timestampAttribute = timestampAttribute;

            this.select = Select.of(outputSchema, filterJson, selectFunctions, flattenField, outputType);
            this.logging = Logging.map(logging);

            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            select.setup();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final long sequence = c.element();
            try {
                final Object value;
                if (!elements.isEmpty()) {
                    value = createElements(elementFieldType, elements, sequence);
                } else {
                    value = createElement(elementFieldType, from, interval, intervalUnit, sequence);
                }
                Map<String, Object> map = createElement(elementFieldType, value, sequence);

                if (select.useSelect()) {
                    map = select.select(map, c.timestamp());
                }

                if (timestampAttribute != null) {
                    final Object eventTimestampValue = map.get(timestampAttribute);
                    final org.joda.time.Instant eventTimestamp = DateTimeUtil.toJodaInstant(eventTimestampValue);
                    MElement output = MElement.builder(map).withEventTime(eventTimestamp).build();
                    output = select.convert(output);
                    c.outputWithTimestamp(output, eventTimestamp);
                    Logging.log(LOG, logging, "output", output);
                } else {
                    MElement output = MElement.of(map, c.timestamp());
                    output = select.convert(output);
                    c.output(output);
                    Logging.log(LOG, logging, "output", output);
                }
            } catch (final Throwable e) {
                //errorCounter.inc();
                final MFailure failure = MFailure
                        .of(jobName, moduleName, "sequence: " + sequence, e, c.timestamp());
                final String errorMessage = String.format("Failed to execute create sequence element for sequence: %d, error: %s", sequence, e.getMessage());
                LOG.error(errorMessage);
                if(failFast) {
                    throw new RuntimeException(errorMessage, e);
                }
                c.output(failuresTag, failure.toElement(c.timestamp()));
            }
        }

    }

    private static Schema createElementSchema(final Schema.FieldType elementFieldType) {
        return switch (elementFieldType.getType()) {
            case element -> elementFieldType.getElementSchema();
            default -> Schema.of(List.of(
                    Schema.Field.of("sequence", Schema.FieldType.INT64),
                    Schema.Field.of("timestamp", Schema.FieldType.TIMESTAMP),
                    Schema.Field.of("value", elementFieldType)
            ));
        };
    }

    private static long calculateElementSize(Parameters parameters) {

        if(!parameters.elements.isEmpty()) {
            return parameters.elements.size();
        }

        if(parameters.to == null) {
            return -1L;
        }
        switch (parameters.elementType) {
            case date -> {
                final ChronoUnit chronoUnit = DateTimeUtil.convertChronoUnit(parameters.intervalUnit);
                final LocalDate fromDate = DateTimeUtil.toLocalDate(parameters.from);
                final LocalDate toDate   = DateTimeUtil.toLocalDate(parameters.to);
                LocalDate currentDate = LocalDate.from(fromDate);
                long count = 0;
                while(currentDate.isBefore(toDate)) {
                    count++;
                    currentDate = currentDate.plus(parameters.interval, chronoUnit);
                }
                if(currentDate.isEqual(toDate)) {
                    count++;
                }
                return count;
            }
            case time -> {
                final ChronoUnit chronoUnit = DateTimeUtil.convertChronoUnit(parameters.intervalUnit);
                final LocalTime fromTime = DateTimeUtil.toLocalTime(parameters.from);
                final LocalTime toTime   = DateTimeUtil.toLocalTime(parameters.to);
                LocalTime currentTime = LocalTime.from(fromTime);
                long count = 0;
                while(currentTime.isBefore(toTime)) {
                    count++;
                    currentTime = currentTime.plus(parameters.interval, chronoUnit);
                }
                if(currentTime.equals(toTime)) {
                    count++;
                }
                return count;
            }
            case timestamp -> {
                final ChronoUnit chronoUnit = DateTimeUtil.convertChronoUnit(parameters.intervalUnit);
                final Instant fromInstant = DateTimeUtil.toInstant(parameters.from);
                final Instant toInstant   = DateTimeUtil.toInstant(parameters.to);
                Instant currentInstant = Instant.from(fromInstant);
                long count = 0;
                while(currentInstant.isBefore(toInstant)) {
                    count++;
                    currentInstant = currentInstant.plus(parameters.interval, chronoUnit);
                }
                if(currentInstant.equals(toInstant)) {
                    count++;
                }
                return count;
            }
            case float16, float32, float64, decimal -> {
                final double fromN = Double.parseDouble(parameters.from);
                final double toN   = Double.parseDouble(parameters.to);
                final Double diff = (toN - fromN + parameters.interval) / parameters.interval;
                return diff.longValue();
            }
            default -> {
                final long fromN = Long.parseLong(parameters.from);
                final long toN   = Long.parseLong(parameters.to);
                return  (toN - fromN + parameters.interval) / parameters.interval;
            }
        }
    }

    private static Object createElements(
            final Schema.FieldType elementFieldType,
            final List<String> elements,
            final Long sequence) {

        final String elementValue = elements.get(sequence.intValue());
        return switch (elementFieldType.getType()) {
            case string -> elementValue;
            case bytes -> ByteBuffer.wrap(Base64.getDecoder().decode(elementValue));
            case date -> Long.valueOf(DateTimeUtil.toLocalDate(elementValue).toEpochDay()).intValue();
            case time -> DateTimeUtil.toLocalTime(elementValue).toNanoOfDay() / 1000L;
            case timestamp -> DateTimeUtil.toJodaInstant(elementValue).getMillis() * 1000L;
            case int16 -> Short.parseShort(elementValue);
            case int32 -> Integer.parseInt(elementValue);
            case int64 -> Long.parseLong(elementValue);
            case float32 -> Float.parseFloat(elementValue);
            case float64 -> Double.parseDouble(elementValue);
            case element -> JsonToElementConverter.convert(elementFieldType.getElementSchema().getFields(), elementValue);
            default -> throw new IllegalArgumentException("Not supported element type: " + elementFieldType.getType());
        };
    }

    private static Object createElement(
            final Schema.FieldType elementFieldType,
            final String from,
            final Integer interval,
            final DateTimeUtil.TimeUnit intervalUnit,
            final Long sequence) {

        return switch (elementFieldType.getType()) {
            case date -> {
                final ChronoUnit chronoUnit = DateTimeUtil.convertChronoUnit(intervalUnit);
                final LocalDate fromDate = DateTimeUtil.toLocalDate(from);
                final long plus = interval * sequence;
                final LocalDate lastDate = fromDate.plus(plus, chronoUnit);
                yield Long.valueOf(lastDate.toEpochDay()).intValue();
            }
            case time -> {
                final ChronoUnit chronoUnit = DateTimeUtil.convertChronoUnit(intervalUnit);
                final LocalTime fromTime = DateTimeUtil.toLocalTime(from);
                final long plus = interval * sequence;
                final LocalTime lastTime = fromTime.plus(plus, chronoUnit);
                yield lastTime.toNanoOfDay() / 1000L;
            }
            case timestamp -> {
                final ChronoUnit chronoUnit = DateTimeUtil.convertChronoUnit(intervalUnit);
                final Instant fromInstant = DateTimeUtil.toInstant(from);
                final long plus = interval * sequence;
                final Instant lastInstant = fromInstant.plus(plus, chronoUnit);
                yield DateTimeUtil.toEpochMicroSecond(lastInstant);
            }
            case float32 -> {
                final float fromN = Float.parseFloat(from);
                yield fromN + interval * sequence;
            }
            case float64 -> {
                final double fromN = Double.parseDouble(from);
                yield fromN + interval * sequence;
            }
            case decimal -> {
                final double fromN = Double.parseDouble(from);
                final double value = fromN + interval * sequence;
                yield BigDecimal.valueOf(value);
            }
            case int32 -> {
                final int fromN = Integer.parseInt(from);
                yield fromN + interval * sequence;
            }
            case int64 -> {
                final long fromN = Long.parseLong(from);
                yield fromN + interval * sequence;
            }
            default -> {
                final long fromN = Long.parseLong(from);
                final long lastN = fromN + interval * sequence;
                yield Long.toString(lastN);
            }
        };
    }

    private static Map<String, Object> createElement(
            final Schema.FieldType elementFieldType,
            final Object value,
            final long sequence) {

        if(Schema.Type.element.equals(elementFieldType.getType())) {
            return (Map<String, Object>) value;
        } else {
            final long epochMicros = DateTimeUtil.toEpochMicroSecond(Instant.now());
            final Map<String, Object> values = new HashMap<>();
            values.put("sequence", sequence);
            values.put("timestamp", epochMicros);
            values.put("value", value);

            return values;
        }
    }

}
