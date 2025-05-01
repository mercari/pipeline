package com.mercari.solution.module;

import org.slf4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Logging implements Serializable {

    private String moduleName;
    private String name;
    private Level level;

    public String getName() {
        return name;
    }

    public Level getLevel() {
        return level;
    }

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public List<String> validate() {
        final List<String> errorMessages = new ArrayList<>();
        if(name == null) {
            errorMessages.add("logging name must not be null");
        }
        return errorMessages;
    }

    public enum Type {
        input,
        output,
        not_matched,
        system
    }

    public enum Level {
        trace,
        debug,
        info,
        warn,
        error
    }

    public static Map<String, Logging> map(List<Logging> loggingList) {
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
        final String logMessage = String.format("%s.%s: %s", logging.getModuleName(), logging.getName(), message);
        switch (logging.level) {
            case trace -> logger.trace(logMessage);
            case debug -> logger.debug(logMessage);
            case info -> logger.info(logMessage);
            case warn -> logger.warn(logMessage);
            case error -> logger.error(logMessage);
            case null -> logger.debug(logMessage);
        }
    }

    public static void log(final Logger logger, final Map<String, Logging> loggings, final String message) {
        if(loggings == null || loggings.isEmpty()) {
            return;
        }
        final Logging logging = loggings.get("system");
        log(logger, logging, message);
    }

}
