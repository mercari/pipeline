package com.mercari.solution.module;

import com.mercari.solution.util.DateTimeUtil;
import org.apache.beam.sdk.transforms.windowing.*;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class Strategy implements Serializable {

    public WindowStrategy window;
    public TriggerStrategy trigger;
    public AccumulationMode mode;

    public List<String> validate() {
        final List<String> errorMessages = new ArrayList<>();
        if(window != null) {
            errorMessages.addAll(window.validate());
        }
        if(trigger != null) {
            errorMessages.addAll(trigger.validate());
        }
        return errorMessages;
    }

    public void setDefaults() {
        if(window != null) {
            window.setDefaults();
        }
        if(trigger != null) {
            trigger.setDefaults();
        }
    }

    public boolean isDefault() {
        return window == null && trigger == null && mode == null;
    }

    public <T> Window<T> createWindow() {
        return createWindow(window, trigger, mode);
    }

    public static <T> Window<T> createWindow(
            final WindowStrategy windowStrategy,
            final TriggerStrategy triggerStrategy,
            final AccumulationMode accumulationMode) {

        Window<T> window;
        if(windowStrategy != null) {
            window = WindowStrategy.createWindow(windowStrategy);
            if(windowStrategy.allowedLateness != null) {
                window = window.withAllowedLateness(DateTimeUtil.getDuration(windowStrategy.unit, windowStrategy.allowedLateness));
            }
            if(windowStrategy.timestampCombiner != null) {
                window = window.withTimestampCombiner(windowStrategy.timestampCombiner);
            }
        } else {
            window = Window
                    .<T>into(new GlobalWindows())
                    .triggering(DefaultTrigger.of());
        }

        if(triggerStrategy != null) {
            window = window.triggering(TriggerStrategy.createTrigger(triggerStrategy));
        }

        window = switch (accumulationMode) {
            case accumulating -> window.accumulatingFiredPanes();
            case discarding -> window.discardingFiredPanes();
            case retracting -> throw new IllegalArgumentException();
            case null -> window.discardingFiredPanes();
        };

        //WindowingStrategy<MElement,String> f = WindowingStrategy.<MElement, Boolean>of(window.getWindowFn()).withMode(WindowingStrategy.AccumulationMode.RETRACTING_FIRED_PANES);

        return window;
    }

    public static <T> Window<T> createDefaultWindow() {
        return Window
                .<T>into(new GlobalWindows())
                .triggering(DefaultTrigger.of());
    }

    public static <T> Window<T> createRepeatedElementWindow() {
        return Window
                .<T>into(new GlobalWindows())
                .triggering(Repeatedly
                        .forever(AfterProcessingTime.pastFirstElementInPane()))
                .withTimestampCombiner(TimestampCombiner.LATEST)
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO);
    }

    public static Strategy createDefaultStrategy() {
        final Strategy strategy = new Strategy();
        return strategy;
    }

    public static class WindowStrategy implements Serializable {

        private WindowType type;
        private DateTimeUtil.TimeUnit unit;
        private Long size;
        private Long period;
        private Long gap;
        private Long offset;
        private String timezone;
        private String startDate;
        private Long allowedLateness;
        private TimestampCombiner timestampCombiner;

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(!WindowType.global.equals(this.type)) {
                if(this.size == null) {
                    if(WindowType.fixed.equals(this.type) || WindowType.sliding.equals(this.type)) {
                        errorMessages.add("Aggregation module.window requires size for fixed or sliding window");
                    }
                }
                if(this.period == null) {
                    if(WindowType.sliding.equals(this.type)) {
                        errorMessages.add("Aggregation module.window requires period for sliding window");
                    }
                }
                if(WindowType.session.equals(this.type)) {
                    if(this.gap == null) {
                        errorMessages.add("Aggregation module.window requires gap for session window");
                    }
                }
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(this.type == null) {
                this.type = WindowType.global;
            }
            if(this.unit == null) {
                this.unit = DateTimeUtil.TimeUnit.second;
            }
            if(this.timezone == null) {
                this.timezone = "UTC";
            }
            if(this.startDate == null) {
                this.startDate = "1970-01-01";
            }
            if(this.offset == null) {
                this.offset = 0L;
            }
            if(this.allowedLateness == null) {
                this.allowedLateness = 0L;
            }
        }

        public enum WindowType implements Serializable {
            global,
            fixed,
            sliding,
            session,
            calendar
        }

        public static <T> Window<T> createWindow(final WindowStrategy windowStrategy) {
            return switch (windowStrategy.type) {
                case global -> Window.into(new GlobalWindows());
                case fixed -> Window.into(FixedWindows
                        .of(DateTimeUtil.getDuration(windowStrategy.unit, windowStrategy.size))
                        .withOffset(DateTimeUtil.getDuration(windowStrategy.unit, windowStrategy.offset)));
                case sliding -> Window.into(SlidingWindows
                        .of(DateTimeUtil.getDuration(windowStrategy.unit, windowStrategy.size))
                        .every(DateTimeUtil.getDuration(windowStrategy.unit, windowStrategy.period))
                        .withOffset(DateTimeUtil.getDuration(windowStrategy.unit, windowStrategy.offset)));
                case session -> Window.into(Sessions
                        .withGapDuration(DateTimeUtil.getDuration(windowStrategy.unit, windowStrategy.gap)));
                case calendar -> {
                    final LocalDate startDate = DateTimeUtil.toLocalDate(windowStrategy.startDate);
                    yield switch (windowStrategy.unit) {
                        case day -> Window.into(CalendarWindows
                                .days(windowStrategy.size.intValue())
                                .withTimeZone(DateTimeZone.forID(windowStrategy.timezone))
                                .withStartingDay(startDate.getYear(), startDate.getMonthValue(), startDate.getDayOfMonth()));
                        case week -> Window.into(CalendarWindows
                                .weeks(windowStrategy.size.intValue(), windowStrategy.offset.intValue())
                                .withTimeZone(DateTimeZone.forID(windowStrategy.timezone))
                                .withStartingDay(startDate.getYear(), startDate.getMonthValue(), startDate.getDayOfMonth()));
                        case month -> Window.into(CalendarWindows
                                .months(windowStrategy.size.intValue())
                                .withTimeZone(DateTimeZone.forID(windowStrategy.timezone))
                                .withStartingMonth(startDate.getYear(), startDate.getMonthValue()));
                        case year -> Window.into(CalendarWindows
                                .years(windowStrategy.size.intValue())
                                .withTimeZone(DateTimeZone.forID(windowStrategy.timezone))
                                .withStartingYear(startDate.getYear()));
                        default -> throw new IllegalArgumentException("Not supported calendar timeunit type: " + windowStrategy.type);
                    };
                }
                default -> throw new IllegalArgumentException("Not supported window type: " + windowStrategy.type);
            };
        }
    }

    public static class TriggerStrategy implements Serializable {

        private TriggerType type;

        // for watermark
        private TriggerStrategy earlyFiringTrigger;
        private TriggerStrategy lateFiringTrigger;

        // for composite triggers
        private List<TriggerStrategy> childrenTriggers;

        // for repeatedly
        private TriggerStrategy foreverTrigger;

        // for afterProcessingTime
        private Long pastFirstElementDelay;
        private DateTimeUtil.TimeUnit pastFirstElementDelayUnit;

        // for afterPane
        private Integer elementCountAtLeast;

        // final trigger
        private TriggerStrategy finalTrigger;

        public List<String> validate() {
            return new ArrayList<>();
        }

        public void setDefaults() {
            if(this.type == null) {
                this.type = TriggerType.afterWatermark;
            }
        }

        public enum TriggerType {
            afterWatermark,
            afterProcessingTime,
            afterPane,
            repeatedly,
            afterEach,
            afterFirst,
            afterAll
        }

        public static Trigger createTrigger(final TriggerStrategy parameter) {
            final Trigger trigger = switch (parameter.type) {
                case afterWatermark -> {
                    if(parameter.earlyFiringTrigger != null && parameter.lateFiringTrigger != null) {
                        final Trigger earlyFiringTrigger =  createTrigger(parameter.earlyFiringTrigger);
                        final Trigger lateFiringTrigger =  createTrigger(parameter.lateFiringTrigger);
                        if(!(earlyFiringTrigger instanceof Trigger.OnceTrigger)) {
                            throw new IllegalArgumentException("AfterWatermark.earlyFiringTrigger must be OnceTrigger, not to be RepeatedTrigger");
                        }
                        if(!(lateFiringTrigger instanceof Trigger.OnceTrigger)) {
                            throw new IllegalArgumentException("AfterWatermark.lateFiringTrigger must be OnceTrigger, not to be RepeatedTrigger");
                        }
                        yield AfterWatermark.pastEndOfWindow()
                                .withEarlyFirings((Trigger.OnceTrigger) earlyFiringTrigger)
                                .withLateFirings((Trigger.OnceTrigger) lateFiringTrigger);
                    } else if(parameter.earlyFiringTrigger != null) {
                        final Trigger earlyFiringTrigger =  createTrigger(parameter.earlyFiringTrigger);
                        if(!(earlyFiringTrigger instanceof Trigger.OnceTrigger)) {
                            throw new IllegalArgumentException("AfterWatermark.earlyFiringTrigger must be OnceTrigger, not to be RepeatedTrigger");
                        }
                        yield AfterWatermark.pastEndOfWindow()
                                .withEarlyFirings((Trigger.OnceTrigger) earlyFiringTrigger);
                    } else if(parameter.lateFiringTrigger != null) {
                        final Trigger lateFiringTrigger =  createTrigger(parameter.lateFiringTrigger);
                        if(!(lateFiringTrigger instanceof Trigger.OnceTrigger)) {
                            throw new IllegalArgumentException("AfterWatermark.lateFiringTrigger must be OnceTrigger, not to be RepeatedTrigger");
                        }
                        yield AfterWatermark.pastEndOfWindow()
                                .withLateFirings((Trigger.OnceTrigger) lateFiringTrigger);
                    } else {
                        yield AfterWatermark.pastEndOfWindow();
                    }
                }
                case afterProcessingTime -> {
                    final AfterProcessingTime afterProcessingTime = AfterProcessingTime.pastFirstElementInPane();
                    if(parameter.pastFirstElementDelay == null || parameter.pastFirstElementDelayUnit == null) {
                        yield afterProcessingTime;
                    }
                    yield afterProcessingTime.plusDelayOf(
                            DateTimeUtil.getDuration(parameter.pastFirstElementDelayUnit, parameter.pastFirstElementDelay));
                }
                case afterPane -> AfterPane.elementCountAtLeast(parameter.elementCountAtLeast);
                case afterFirst, afterEach, afterAll -> {
                    final List<Trigger> triggers = new ArrayList<>();
                    for(final TriggerStrategy child : parameter.childrenTriggers) {
                        triggers.add(createTrigger(child));
                    }
                    yield switch (parameter.type) {
                        case afterFirst -> AfterFirst.of(triggers);
                        case afterEach -> AfterEach.inOrder(triggers);
                        case afterAll -> AfterAll.of(triggers);
                        default -> throw new IllegalArgumentException("Not supported window trigger: " + parameter.type);
                    };
                }
                case repeatedly -> Repeatedly.forever(createTrigger(parameter.foreverTrigger));
                default -> throw new IllegalArgumentException("Not supported window trigger: " + parameter.type);
            };

            if(parameter.finalTrigger != null) {
                final Trigger finalTrigger = createTrigger(parameter.finalTrigger);
                if(!(finalTrigger instanceof Trigger.OnceTrigger)) {
                    throw new IllegalArgumentException("FinalTrigger must be OnceTrigger, not to be RepeatedTrigger");
                }
                return trigger.orFinally((Trigger.OnceTrigger) finalTrigger);
            }

            return trigger;
        }

    }

    public enum AccumulationMode {
        discarding,
        accumulating,
        retracting
    }

}
