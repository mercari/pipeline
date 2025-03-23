package com.mercari.solution.module;

import org.slf4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Logging implements Serializable {

    private String name;
    private Level level;

    public String getName() {
        return name;
    }

    public Level getLevel() {
        return level;
    }

    public List<String> validate() {
        final List<String> errorMessages = new ArrayList<>();
        if(name == null) {
            errorMessages.add("logging name must not be null");
        }
        return errorMessages;
    }

    public enum Level {
        trace,
        debug,
        info,
        warn,
        error
    }

    public static Map<String, Logging> of(List<Logging> loggingList) {
        return loggingList.stream().collect(Collectors.toMap(Logging::getName, p -> p));
    }

    public static void log(final Logger logger, final Map<String, Logging> loggings, final String name, final MElement element) {
        if(loggings == null || loggings.isEmpty()) {
            return;
        }
        final Logging logging = loggings.get(name);
        log(logger, logging, element);
    }

    public static void log(final Logger logger, final Map<String, Logging> loggings, final String name, final String message) {
        if(loggings == null || loggings.isEmpty()) {
            return;
        }
        final Logging logging = loggings.get(name);
        log(logger, logging, message);
    }

    public static void log(final Logger logger, final Logging logging, final MElement element) {
        if(element == null) {
            return;
        }
        try {
            final String message = element.toString();
            log(logger, logging, message);
        } catch (Throwable e) {
            logger.error("failed to log for logging: {}, cause: {}", logging.name, e.getMessage());
        }
    }

    public static void log(final Logger logger, final Logging logging, final String message) {
        if(logger == null || logging == null || message == null) {
            return;
        }
        switch (logging.level) {
            case trace -> logger.trace(message);
            case debug -> logger.debug(message);
            case info -> logger.info(message);
            case warn -> logger.warn(message);
            case error -> logger.error(message);
            case null -> logger.debug(message);
        }
    }

}
