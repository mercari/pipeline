package com.mercari.solution.util;

import com.mercari.solution.module.Schema;
import freemarker.core.Environment;
import freemarker.ext.beans.BeansWrapper;
import freemarker.template.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TemplateUtil {

    private static final Map<String, Object> UTILS = Map.of(
            "string", new StringFunctions(),
            "datetime", new DateTimeFunctions(),
            "bigtable", new BigtableFunctions()
    );

    public static Template createSafeTemplate(final String name, final String template) {

        final Configuration templateConfig = new Configuration(Configuration.VERSION_2_3_34);
        templateConfig.setNumberFormat("computer");
        templateConfig.setTemplateExceptionHandler(new ImputeSameVariablesTemplateExceptionHandler());
        templateConfig.setLogTemplateExceptions(false);
        templateConfig.setSharedVariable("statics", BeansWrapper.getDefaultInstance().getStaticModels());
        try {
            templateConfig.setSharedVariable("utils", UTILS);
            return new Template(name, new StringReader(template), templateConfig);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static Template createStrictTemplate(final String name, final String template) {
        final Configuration templateConfig = new Configuration(Configuration.VERSION_2_3_34);
        templateConfig.setNumberFormat("computer");
        templateConfig.setSharedVariable("statics", BeansWrapper.getDefaultInstance().getStaticModels());
        //templateConfig.setObjectWrapper(new CSVWrapper(Configuration.VERSION_2_3_30));
        try {
            templateConfig.setSharedVariable("utils", UTILS);
            return new Template(name, new StringReader(template), templateConfig);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static String executeStrictTemplate(final String templateText, final Map<?, ?> data) {
        final Template template = createStrictTemplate("template", templateText);
        return executeStrictTemplate(template, data);
    }

    public static String executeStrictTemplate(final Template template, final Map<?, ?> data) {
        try(final StringWriter writer = new StringWriter()) {
            template.process(data, writer);
            return writer.toString();
        } catch (IOException | TemplateException e) {
            throw new RuntimeException("Failed to process template: " + template + ", for data: " + data + ", cause: " + e.getMessage(), e);
        }
    }

    public static boolean isTemplateText(final String text) {
        if(text == null) {
            return false;
        }
        return text.contains("${") && text.contains("}");
    }

    public static List<String> extractTemplateArgs(final String text, final Schema inputSchema) {
        return extractTemplateArgs(text, inputSchema.getFields());
    }

    public static List<String> extractTemplateArgs(final String text, final List<Schema.Field> fields) {
        final List<String> args = new ArrayList<>();
        if(!isTemplateText(text)) {
            return args;
        }
        for(final com.mercari.solution.module.Schema.Field field : fields) {
            if(text.contains(field.getName()) && !args.contains(field.getName())) {
                args.add(field.getName());
            }
        }
        return args;
    }

    public static void setContextVariables(
            final DoFn.ProcessContext c,
            final Map<String, Object> values) {

        final Map<String, Object> contextValues = new HashMap<>();
        contextValues.put("timestamp", DateTimeUtil.toInstant(c.timestamp().getMillis() * 1000L));
        values.put("context", contextValues);
    }

    public static void setFunctions(final Map<String, Object> values) {
        setFunctions(values, "__");
    }

    public static void setFunctions(final Map<String, Object> values, final String prefix) {
        values.put(prefix + "StringUtils", new StringFunctions());
        values.put(prefix + "DateTimeUtils", new DateTimeFunctions());
    }

    public static class StringFunctions {

        public String format(String format, Object... args) {
            return String.format(format, args);
        }

        public String reverse(String text) {
            if(text == null) {
                return "";
            }
            return new StringBuilder(text).reverse().toString();
        }

    }

    public static class DateTimeFunctions {

        public String formatTimestamp(Long epocMicros) {
            return formatTimestamp(epocMicros, null, null);
        }

        public String formatTimestamp(Instant timestamp) {
            return formatTimestamp(timestamp, null, null);
        }

        public String formatTimestamp(Long epocMicros, String pattern) {
            return formatTimestamp(epocMicros, pattern,null);
        }

        public String formatTimestamp(Instant timestamp, String pattern) {
            return formatTimestamp(timestamp, pattern,null);
        }

        public String formatTimestamp(Long epocMicros, String pattern, String timezone) {
            if(epocMicros == null) {
                return "";
            }
            final Instant timestamp = DateTimeUtil.toInstant(epocMicros);
            return formatTimestamp(timestamp, pattern, timezone);
        }

        public String formatTimestamp(Instant timestamp, String pattern, String timezone) {
            if(timestamp == null) {
                return "";
            }
            final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
            final DateTimeFormatter formatter;
            if(pattern == null) {
                formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");
            } else {
                formatter = DateTimeFormatter.ofPattern(pattern);
            }
            return dateTime.format(formatter);
        }

        public String formatDate(Integer epocDays) {
            return formatDate(epocDays, null);
        }

        public String formatDate(LocalDate localDate) {
            return formatDate(localDate, null);
        }

        public String formatDate(Integer epocDays, String pattern) {
            if(epocDays == null) {
                return "";
            }
            final LocalDate localDate = LocalDate.ofEpochDay(epocDays);
            return formatDate(localDate, pattern);
        }

        public String formatDate(LocalDate localDate, String pattern) {
            if(localDate == null) {
                return "";
            }
            final DateTimeFormatter formatter;
            if(pattern == null) {
                formatter = DateTimeFormatter.ISO_DATE;
            } else {
                formatter = DateTimeFormatter.ofPattern(pattern);
            }
            return localDate.format(formatter);
        }

        public String formatTime(Long epocMicros) {
            return formatTime(epocMicros, null);
        }

        public String formatTime(LocalTime localTime) {
            return formatTime(localTime, null);
        }

        public String formatTime(Long epocMicros, String pattern) {
            if(epocMicros == null) {
                return "";
            }
            final LocalTime localTime = LocalTime.ofNanoOfDay(epocMicros * 1000L);
            return formatTime(localTime, pattern);
        }

        public String formatTime(LocalTime localTime, String pattern) {
            if(localTime == null) {
                return "";
            }
            final DateTimeFormatter formatter;
            if(pattern == null) {
                formatter = DateTimeFormatter.ISO_TIME;
            } else {
                formatter = DateTimeFormatter.ofPattern(pattern);
            }
            return localTime.format(formatter);
        }

        public LocalDate currentDate(final String zone) {
            return currentDate(zone, null, null);
        }

        public LocalDate currentDate(final String zone, final Long plusDays) {
            return currentDate(zone, plusDays, "DAYS");
        }

        public LocalDate currentDate(final String zone, final Long plusAmount, final String unit) {
            LocalDate localDate;
            if(zone == null) {
                localDate = LocalDate.now(ZoneOffset.UTC);
            } else {
                localDate = LocalDate.now(ZoneId.of(zone));
            }
            if(plusAmount != null && unit != null) {
                final ChronoUnit chronoUnit = ChronoUnit.valueOf(unit);
                localDate = localDate.plus(plusAmount, chronoUnit);
            }
            return localDate;
        }

        public LocalTime currentTime(final String zone) {
            return currentTime(zone, null, null);
        }

        public LocalTime currentTime(final String zone, final Long plusSeconds) {
            return currentTime(zone, plusSeconds, "SECONDS");
        }

        public LocalTime currentTime(final String zone, final Long plusAmount, final String unit) {
            LocalTime localTime;
            if(zone == null) {
                localTime = LocalTime.now(ZoneOffset.UTC);
            } else {
                localTime = LocalTime.now(ZoneId.of(zone));
            }
            if(plusAmount != null && unit != null) {
                final ChronoUnit chronoUnit = ChronoUnit.valueOf(unit);
                localTime = localTime.plus(plusAmount, chronoUnit);
            }
            return localTime;
        }

        public String currentDateTime(final String zone) {
            return currentDateTime(zone, 0L);
        }

        public String currentDateTime(final String zone, final Long plusSeconds) {
            return currentDateTime(zone, plusSeconds, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        }

        public String currentDateTime(final String zone, final Long plusSeconds, final String pattern) {
            final LocalDateTime localDateTime = LocalDateTime.now(ZoneId.of(zone));
            return DateTimeFormatter.ofPattern(pattern).format(localDateTime.plusSeconds(plusSeconds));
        }

        public Instant currentTimestamp() {
            return currentTimestamp(null, null, null);
        }

        public Instant currentTimestamp(final Long plusSeconds) {
            return currentTimestamp(plusSeconds, ChronoUnit.SECONDS.name(), null);
        }

        public Instant currentTimestamp(final Long plusAmount, final String unit) {
            return currentTimestamp(plusAmount, unit, null);
        }

        public Instant currentTimestamp(final String truncateUnit) {
            return currentTimestamp(null, null, truncateUnit);
        }

        public Instant currentTimestamp(final Long plusAmount, final String unit, final String truncateUnit) {
            LocalDateTime instant = LocalDateTime.now(ZoneOffset.UTC);
            if(plusAmount != null && unit != null) {
                final ChronoUnit chronoUnit = ChronoUnit.valueOf(unit);
                instant = instant.plus(plusAmount, chronoUnit);
            }
            if(truncateUnit != null) {
                final ChronoUnit chronoUnit = ChronoUnit.valueOf(truncateUnit);
                instant = instant.truncatedTo(chronoUnit);
            }
            return instant.toInstant(ZoneOffset.UTC);
        }

        public String year(Instant timestamp, String timezone) {
            if(timestamp == null) {
                return "";
            }
            final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
            return Integer.valueOf(dateTime.getYear()).toString();
        }

        public String month(Instant timestamp, String timezone) {
            if(timestamp == null) {
                return "";
            }
            final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
            return Integer.valueOf(dateTime.getMonthValue()).toString();
        }

        public String month(Instant timestamp, String timezone, Integer padding) {
            if(timestamp == null) {
                return "";
            }
            final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
            return String.format("%0" + padding + "d", Integer.valueOf(dateTime.getMonthValue()));
        }

        public String day(Instant timestamp, String timezone) {
            if(timestamp == null) {
                return "";
            }
            final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
            return Integer.valueOf(dateTime.getDayOfMonth()).toString();
        }

        public String day(Instant timestamp, String timezone, Integer padding) {
            if(timestamp == null) {
                return "";
            }
            final LocalDateTime dateTime = getLocalDateTime(timestamp, timezone);
            return String.format("%0" + padding + "d", Integer.valueOf(dateTime.getDayOfMonth()));
        }

        private static LocalDateTime getLocalDateTime(final Instant timestamp, final String timezone) {
            if(timezone == null) {
                return LocalDateTime.ofInstant(timestamp, ZoneOffset.UTC);
            } else {
                return LocalDateTime.ofInstant(timestamp, ZoneId.of(timezone));
            }
        }

    }

    public static class BigtableFunctions {

        public Long reverseTimestampMicros(final Instant instant) {
            return reverseTimestampMicros(DateTimeUtil.toEpochMicroSecond(instant));
        }

        public Long reverseTimestampMicros(final Long epochMicros) {
            return Long.MAX_VALUE - epochMicros;
        }

        public Long reverseTimestampMillis(final Instant instant) {
            return reverseTimestampMillis(DateTimeUtil.reduceAccuracy(DateTimeUtil.toEpochMicroSecond(instant), 1000));
        }

        public Long reverseTimestampMillis(final Long epochMillis) {
            return (Long.MAX_VALUE / 1000) - epochMillis;
        }

    }

    static class ImputeSameVariablesTemplateExceptionHandler implements TemplateExceptionHandler {

        @Override
        public void handleTemplateException(TemplateException te, Environment env, java.io.Writer out) {
            try {
                if(te instanceof TemplateModelException e) {
                    final List<String> lines = env.getCurrentTemplate().toString().lines().collect(Collectors.toList());
                    final String line = lines.get(te.getLineNumber() - 1);
                    final String content = line.substring(te.getColumnNumber()-1, te.getEndColumnNumber());
                    out.write(content);
                    return;
                }
                if(te.getBlamedExpressionString() == null) {
                    throw new IllegalArgumentException(te);
                }
                final List<String> lines = env.getCurrentTemplate().toString().lines().collect(Collectors.toList());
                final String line = lines.get(te.getLineNumber() - 1);
                final String prefix = line.substring(te.getColumnNumber()-2, te.getColumnNumber()-1);
                final String suffix = line.substring(te.getEndColumnNumber(), te.getEndColumnNumber()+1);
                if("{".equals(prefix) && "}".equals(suffix)) {
                    out.write("${" + te.getBlamedExpressionString() + "}");
                } else if("{".equals(prefix) && line.contains("}")) {
                    final int start = te.getColumnNumber()-1;
                    final String target = line.substring(start, line.indexOf("}", start));
                    out.write("${" + target.replaceAll("\"", "'") + "}");
                } else {
                    out.write("${" + te.getBlamedExpressionString() + "}");
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    static class CSVWrapper extends DefaultObjectWrapper {

        CSVWrapper(Version incompatibleImprovements) {
            super(incompatibleImprovements);
        }

        @Override
        protected TemplateModel handleUnknownType(Object obj) throws TemplateModelException {
            if (obj instanceof CSVRecord) {
                return new CSVRecordModel((CSVRecord) obj);
            }
            return super.handleUnknownType(obj);
        }

        public class CSVRecordModel implements TemplateHashModel {
            private final CSVRecord csvRecord;

            public CSVRecordModel(CSVRecord csvRecord) {
                this.csvRecord = csvRecord;
            }

            @Override
            public TemplateModel get(String key) throws TemplateModelException {
                return wrap(csvRecord.get(key));
            }

            @Override
            public boolean isEmpty() {
                return csvRecord.size() == 0;
            }

        }

    }

}
