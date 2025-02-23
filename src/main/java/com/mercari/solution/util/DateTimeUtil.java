package com.mercari.solution.util;

import com.google.cloud.Timestamp;
import com.google.type.TimeOfDay;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.*;
import org.joda.time.Duration;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DateTimeUtil {

    private static final DateTimeFormatter FORMAT_DATE1 = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter FORMAT_DATE2 = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter FORMAT_DATE3 = DateTimeFormatter.ofPattern("yyyy/MM/dd");
    private static final DateTimeFormatter FORMAT_TIME1 = DateTimeFormatter.ofPattern("HH:mm");
    private static final DateTimeFormatter FORMAT_TIME2 = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final Pattern PATTERN_DATE1 = Pattern
            .compile("^([1][9][0-9]{2}|[2][0][0-9]{2})([0][1-9]|[1][0-2])([0][1-9]|[1-2][0-9]|[3][0-1])$");
    private static final Pattern PATTERN_DATE2 = Pattern
            .compile("^[0-3][0-9]{3}-([0][1-9]|[1][0-2]|[0-9])-([0][1-9]|[1-2][0-9]|[3][0-1]|[0-9])$");
    private static final Pattern PATTERN_DATE3 = Pattern
            .compile("^[0-3][0-9]{3}/([0][1-9]|[1][0-2]|[0-9])/([0][1-9]|[1-2][0-9]|[3][0-1]|[0-9])$");

    private static final Pattern PATTERN_TIME1 = Pattern
            .compile("^([0-1][0-9]|[2][0-3]):[0-5][0-9]$");
    private static final Pattern PATTERN_TIME2 = Pattern
            .compile("^([0-1][0-9]|[2][0-3]):[0-5][0-9]:[0-5][0-9]$");
    private static final Pattern PATTERN_TIME3 = Pattern
            .compile("^([0-1][0-9]|[2][0-3]):[0-5][0-9]:[0-5][0-9][,\\.][0-9]{1,9}$");

    private static final Pattern PATTERN_TIMESTAMP = Pattern
            .compile("^[0-3][0-9]{3}[-/]([0][1-9]|[1][0-2]|[0-9])[-/]([0][1-9]|[1-2][0-9]|[3][0-1]|[0-9])[ Tt]"
                    + "([0-1][0-9]|[2][0-3])[:/ ][0-5][0-9][:/ ][0-5][0-9]([,\\.][0-9]{1,9})?"
                    + "([Zz]?|[-+][01]?[0-9][:]?[0-9]{1,2})$");
    private static final Pattern PATTERN_TIMESTAMP_NANO = Pattern
            .compile("([,\\.][0-9]{1,9})");
    private static final Pattern PATTERN_TIMESTAMP_ZONE = Pattern
            .compile("([Zz]|[-+][01]?[0-9][:]?[0-9]{1,2})$");

    public static boolean isDate(final String text) {
        return PATTERN_DATE3.matcher(text).find()
                || PATTERN_DATE2.matcher(text).find()
                || PATTERN_DATE1.matcher(text).find();
    }

    public static boolean isTime(final String text) {
        return PATTERN_TIME3.matcher(text).find()
                || PATTERN_TIME2.matcher(text).find()
                || PATTERN_TIME1.matcher(text).find();
    }

    public static boolean isTimestamp(final String text) {
        return PATTERN_TIMESTAMP.matcher(text).find();
    }

    public static LocalTime toLocalTime(final String text) {
        return toLocalTime(text, false);
    }
    public static LocalTime toLocalTime(final String text, final boolean safe) {
        final Matcher matcher = PATTERN_TIME3.matcher(text);
        if(matcher.find()) {
            final String group = matcher.group();
            final String[] values;
            if(group.contains(".")) {
                values = group.split("\\.");
            } else {
                values = group.split(",");
            }
            final Integer secondOfDay = Integer.valueOf(LocalTime.parse(values[0], FORMAT_TIME2).toSecondOfDay());
            final String nanoOfDay = StringUtils.rightPad(values[1], 9, "0");
            return LocalTime.ofNanoOfDay(secondOfDay.longValue() * 1000_000_000 + Long.valueOf(nanoOfDay));
        } else if(PATTERN_TIME2.matcher(text).find()) {
            return LocalTime.parse(text, FORMAT_TIME2);
        } else if(PATTERN_TIME1.matcher(text).find()) {
            return LocalTime.parse(text, FORMAT_TIME1);
        } else {
            if(safe) {
                return null;
            }
            throw new IllegalArgumentException("Illegal time string: " + text);
        }
    }

    public static LocalDate toLocalDate(final String text) {
        return toLocalDate(text, false);
    }

    public static LocalDate toLocalDate(final String text, final boolean safe) {
        if(PATTERN_DATE3.matcher(text).find()) {
            return LocalDate.parse(text, FORMAT_DATE3);
        } else if(PATTERN_DATE2.matcher(text).find()) {
            return LocalDate.parse(text, FORMAT_DATE2);
        } else if(PATTERN_DATE1.matcher(text).find()) {
            return LocalDate.parse(text, FORMAT_DATE1);
        } else {
            if(safe) {
                return null;
            }
            throw new IllegalArgumentException("Illegal time string: " + text);
        }
    }

    public static LocalDate toLocalDate(final com.google.cloud.Date date) {
        return LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth());
    }

    public static Instant toInstant(final Object value) {
        return switch (value) {
            case String s -> toInstant(s);
            case Number n -> toInstant(n.longValue());
            case Instant i -> i;
            case org.joda.time.Instant i -> Instant.ofEpochMilli(i.getMillis());
            case com.google.cloud.Timestamp t -> Instant.ofEpochSecond(t.getSeconds(), t.getNanos());
            case null -> null;
            default -> throw new IllegalArgumentException("Could not convert value: " + value + ", class: " + value.getClass() + " to java.time.Instant");
        };
    }

    public static Instant toInstant(final Long microSeconds) {
        if(microSeconds == null) {
            return null;
        }
        long second = microSeconds / 1000_000L;
        long nano   = microSeconds % 1000_000L * 1000L;
        return Instant.ofEpochSecond(second, Math.toIntExact(nano));
    }

    public static Instant toInstant(final String text) {
        return toInstant(text, false);
    }

    public static Instant toInstant(final String text, final boolean safe) {
        try {
            return ZonedDateTime.parse(text).toInstant();
        } catch (DateTimeParseException e) {
            if(PATTERN_TIMESTAMP.matcher(text).find()) {
                LocalDateTime localDateTime = LocalDateTime.of(
                        Integer.valueOf(text.substring(0, 4)),
                        Integer.valueOf(text.substring(5, 7)),
                        Integer.valueOf(text.substring(8, 10)),
                        Integer.valueOf(text.substring(11, 13)),
                        Integer.valueOf(text.substring(14, 16)),
                        Integer.valueOf(text.substring(17, 19)));

                Matcher matcher = PATTERN_TIMESTAMP_NANO.matcher(text);
                if (matcher.find()) {
                    final String nanoOfDay = StringUtils.rightPad(matcher.group().substring(1), 9, "0");
                    localDateTime = localDateTime.withNano(Integer.valueOf(nanoOfDay));
                }

                matcher = PATTERN_TIMESTAMP_ZONE.matcher(text);
                if (matcher.find()) {
                    final String zone = matcher.group();
                    if ("Z".equals(zone)) {
                        return localDateTime.toInstant(ZoneOffset.UTC);
                    } else {
                        final String time = zone
                                .replaceFirst("\\+", "")
                                .replaceFirst("-", "")
                                .replaceAll(":", "");
                        final String hour = time.substring(0, 2);
                        final String minute = time.substring(2);
                        if (zone.startsWith("-")) {
                            return localDateTime.toInstant(ZoneOffset.ofHoursMinutes(-Integer.valueOf(hour), -Integer.valueOf(minute)));
                        } else {
                            return localDateTime.toInstant(ZoneOffset.ofHoursMinutes(Integer.valueOf(hour), Integer.valueOf(minute)));
                        }
                    }
                }

                return localDateTime.toInstant(ZoneOffset.UTC);
            } else if(PATTERN_DATE3.matcher(text).find() || PATTERN_DATE2.matcher(text).find()) {
                return LocalDateTime.of(
                        Integer.valueOf(text.substring(0, 4)),
                        Integer.valueOf(text.substring(5, 7)),
                        Integer.valueOf(text.substring(8, 10)),
                        0, 0, 0, 0)
                        .toInstant(ZoneOffset.UTC);
            } else if(PATTERN_DATE1.matcher(text).find()) {
                return LocalDateTime.of(
                        Integer.valueOf(text.substring(0, 4)),
                        Integer.valueOf(text.substring(4, 6)),
                        Integer.valueOf(text.substring(6, 8)),
                        0, 0, 0, 0)
                        .toInstant(ZoneOffset.UTC);
            } else {
                if(safe) {
                    return null;
                }
                throw new IllegalArgumentException("Illegal timestamp string: " + text);
            }
        }
    }

    public static org.joda.time.Instant toJodaInstant(final Object value) {
        final Instant timestamp = toInstant(value);
        final long epochMillis = toEpochMicroSecond(timestamp) / 1000L;
        return org.joda.time.Instant.ofEpochMilli(epochMillis);
    }

    public static org.joda.time.Instant toJodaInstant(final Instant instant) {
        if(instant == null) {
            return null;
        }
        final long epochMicros = toEpochMicroSecond(instant.getEpochSecond(), instant.getNano());
        return org.joda.time.Instant.ofEpochMilli(epochMicros / 1000L);
    }

    public static org.joda.time.Instant toJodaInstant(final String text) {
        if(text == null) {
            return null;
        }
        if(StringUtils.isNumeric(text)) {
            return toJodaInstant(Long.valueOf(text));
        }
        final Instant instant = toInstant(text);
        return toJodaInstant(instant);
    }

    public static org.joda.time.Instant toJodaInstant(Long epoch) {
        if(epoch == null) {
            return null;
        }
        epoch = assumeEpochMilliSecond(epoch);
        return org.joda.time.Instant.ofEpochMilli(epoch);
    }

    public static org.joda.time.Instant toJodaInstant(Double epoch) {
        if(epoch == null) {
            return null;
        }
        final BigDecimal decimal = BigDecimal.valueOf(epoch);
        final long second = decimal.longValue();
        final int nanos = decimal.subtract(BigDecimal.valueOf(decimal.intValue())).intValue();
        final Long epochMicros = toEpochMicroSecond(second, nanos);
        return toJodaInstant(epochMicros);
    }

    public static org.joda.time.Instant toJodaInstant(final Timestamp timestamp) {
        if(timestamp == null) {
            return null;
        }
        final Long epochMicroSecond = toEpochMicroSecond(timestamp);
        return org.joda.time.Instant.ofEpochMilli(epochMicroSecond / 1000L);
    }

    public static org.joda.time.Instant toJodaInstant(final com.google.protobuf.Timestamp timestamp) {
        if(timestamp == null) {
            return null;
        }
        final Long epochMicroSecond = toEpochMicroSecond(timestamp);
        return org.joda.time.Instant.ofEpochMilli(epochMicroSecond / 1000L);
    }

    public static com.google.protobuf.Timestamp toProtoTimestamp(final Instant instant) {
        if(instant == null) {
            return null;
        }
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    public static com.google.protobuf.Timestamp toProtoTimestamp(final org.joda.time.Instant instant) {
        if(instant == null) {
            return null;
        }
        long second = instant.getMillis() / 1000L;
        long nano   = instant.getMillis() % 1000L * 1000_000L;
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(second)
                .setNanos(Math.toIntExact(nano))
                .build();
    }

    public static com.google.protobuf.Timestamp toProtoTimestamp(final Long epocMicros) {
        if(epocMicros == null) {
            return null;
        }
        long second = epocMicros / 1000_000L;
        long nano   = epocMicros % 1000_000L * 1000L;
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(second)
                .setNanos(Math.toIntExact(nano))
                .build();
    }

    public static com.google.protobuf.Timestamp toProtoTimestamp(final String text) {
        if(text == null) {
            return null;
        }
        final Instant instant = toInstant(text);
        return toProtoTimestamp(instant);
    }

    public static Integer toEpochDay(final Date date) {
        if(date == null) {
            return null;
        }
        return Long.valueOf(LocalDate
                    .of(date.getYear(), date.getMonth() + 1, date.getDate())
                    .toEpochDay())
                .intValue();
    }

    public static Integer toEpochDay(final java.sql.Date date) {
        if(date == null) {
            return null;
        }
        return (int)date.toLocalDate().toEpochDay();
    }

    public static Integer toEpochDay(final com.google.cloud.Date date) {
        if(date == null) {
            return null;
        }
        return Long.valueOf(LocalDate.of(date.getYear(), date.getMonth(), date.getDayOfMonth()).toEpochDay()).intValue();
    }

    public static Integer toEpochDay(final com.google.type.Date date) {
        if(date == null) {
            return null;
        }
        long epochDay = LocalDate.of(date.getYear(), date.getMonth(), date.getDay()).toEpochDay();
        return Long.valueOf(epochDay).intValue();
    }

    public static Integer toEpochDay(final String text) {
        if(text == null) {
            return null;
        }
        final LocalDate localDate = toLocalDate(text);
        if(localDate == null) {
            return null;
        }
        long epochDay = LocalDate.of(localDate.getYear(), localDate.getMonth(), localDate.getDayOfMonth()).toEpochDay();
        return Long.valueOf(epochDay).intValue();
    }

    public static Integer toMilliOfDay(final LocalTime localTime) {
        if(localTime == null) {
            return null;
        }
        return Long.valueOf(localTime.toNanoOfDay() / 1000_000).intValue();
    }

    public static Long toMicroOfDay(final LocalTime localTime) {
        if(localTime == null) {
            return null;
        }
        return localTime.toNanoOfDay() / 1000;
    }

    public static Long toMicroOfDay(final TimeOfDay timeOfDay) {
        if(timeOfDay == null) {
            return null;
        }
        final LocalTime localTime = LocalTime
                .of(timeOfDay.getHours(), timeOfDay.getMinutes(), timeOfDay.getSeconds(), timeOfDay.getNanos());
        return toMicroOfDay(localTime);
    }

    public static Long toMicroOfDay(final String text) {
        if(text == null) {
            return null;
        }
        final LocalTime localTime = toLocalTime(text);
        return toMicroOfDay(localTime);
    }

    public static Long toEpochMicroSecond(final ReadableDateTime datetime) {
        if(datetime == null) {
            return null;
        }
        return datetime.toInstant().getMillis() * 1000;
    }

    public static Long toEpochMicroSecond(final Timestamp timestamp) {
        if(timestamp == null) {
            return null;
        }
        return toEpochMicroSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    public static Long toEpochMicroSecond(final com.google.protobuf.Timestamp timestamp) {
        if(timestamp == null) {
            return null;
        }
        return toEpochMicroSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    public static Long toEpochMicroSecond(final Instant instant) {
        if(instant == null) {
            return null;
        }
        return toEpochMicroSecond(instant.getEpochSecond(), instant.getNano());
    }

    public static long toEpochMicroSecond(final long seconds, final int nanos) {
        return seconds * 1000_000 + nanos / 1000;
    }

    public static long toEpochMicroSecond(Double epoch) {
        if(epoch == null) {
            return 0L;
        }
        final BigDecimal decimal = BigDecimal.valueOf(epoch);
        final long second = decimal.longValue();
        final int nanos = decimal.subtract(BigDecimal.valueOf(decimal.intValue())).intValue();
        return toEpochMicroSecond(second, nanos);
    }

    public static Long toEpochMicroSecond(final String text) {
        if(text == null) {
            return null;
        }
        final Instant instant = toInstant(text);
        return toEpochMicroSecond(instant);
    }

    public static Long reduceAccuracy(final Long value, final long n) {
        if(value == null) {
            return null;
        }
        return (value / n) * n;
    }


    public static Long assumeEpochMicroSecond(final Long epoch) {
        if(epoch == null) {
            return null;
        }
        return switch (assumeEpochType(epoch)) {
            case NANO -> epoch / 1000L;
            case MICROS -> epoch;
            case MILLIS -> epoch * 1000L;
            case SECOND -> epoch * 1000_000L;
            default -> null;
        };
    }

    public static Long assumeEpochMilliSecond(final Long epoch) {
        if(epoch == null) {
            return null;
        }
        return switch (assumeEpochType(epoch)) {
            case NANO -> epoch / 1000_000L;
            case MICROS -> epoch / 1000L;
            case MILLIS -> epoch;
            case SECOND -> epoch * 1000L;
            default -> null;
        };
    }

    public static LocalDateTime toLocalDateTime(final Long microSeconds) {
        if(microSeconds == null) {
            return null;
        }
        long second = microSeconds / 1000_000L;
        long nano   = microSeconds % 1000_000L * 1000L;
        return LocalDateTime.ofEpochSecond(second, Math.toIntExact(nano), ZoneOffset.UTC);
    }

    public static class DateTimeTemplateUtils implements Serializable {

        public String formatTimestamp(final Instant instant, final String pattern) {
            return formatTimestamp(instant, pattern, "UTC");
        }

        public String formatTimestamp(final Instant instant, final String pattern, final String zoneId) {
            return DateTimeFormatter.ofPattern(pattern).withZone(ZoneId.of(zoneId)).format(instant);
        }

        public String formatDate(final LocalDate date, final String pattern) {
            return DateTimeFormatter.ofPattern(pattern).format(date);
        }

        public String formatTime(final LocalTime time, final String pattern) {
            return DateTimeFormatter.ofPattern(pattern).format(time);
        }

    }

    private static EpochType assumeEpochType(final Long epoch) {
        if(epoch == null) {
            return EpochType.UNKNOWN;
        }
        final long l = Math.abs(epoch);
        if(l > 50_000_000_000_000_000L) {
            return EpochType.NANO;
        } else if(l > 50_000_000_000_000L) {
            return EpochType.MICROS;
        } else if(l > 5_000_000_000L) {
            return EpochType.MILLIS;
        } else {
            return EpochType.SECOND;
        }
    }

    public static Duration getDuration(final TimeUnit unit, final Long size) {
        return switch (unit) {
            case second -> Duration.standardSeconds(size);
            case minute -> Duration.standardMinutes(size);
            case hour -> Duration.standardHours(size);
            case day -> Duration.standardDays(size);
            default -> throw new IllegalArgumentException("Illegal window unit: " + unit);
        };
    }

    public static ChronoUnit convertChronoUnit(final TimeUnit unit) {
        return switch (unit) {
            case second -> ChronoUnit.SECONDS;
            case minute -> ChronoUnit.MINUTES;
            case hour -> ChronoUnit.HOURS;
            case day -> ChronoUnit.DAYS;
            case week -> ChronoUnit.WEEKS;
            case month -> ChronoUnit.MONTHS;
            case year -> ChronoUnit.YEARS;
        };
    }

    public enum TimeUnit implements Serializable {
        second,
        minute,
        hour,
        day,
        week,
        month,
        year
    }

    private enum EpochType {
        UNKNOWN,
        SECOND,
        MILLIS,
        MICROS,
        NANO
    }

}
