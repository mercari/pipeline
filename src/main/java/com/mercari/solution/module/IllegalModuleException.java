package com.mercari.solution.module;

import java.util.List;

public class IllegalModuleException extends RuntimeException {

    public final String name;
    public final String module;
    public final List<String> errorMessages;

    public IllegalModuleException(final String errorMessage) {
        this(List.of(errorMessage));
    }

    public IllegalModuleException(final List<String> errorMessages) {
        this(null, null, errorMessages);
    }

    public IllegalModuleException(final String errorMessage, final Throwable e) {
        this(errorMessage + ", cause: " + convertThrowableMessage(e));
    }

    public IllegalModuleException(final String name, final String module, final Throwable e) {
        this(name, module, List.of(convertThrowableMessage(e)));
    }

    public IllegalModuleException(final String name, final String module, final List<String> errorMessages) {
        super("name: " + name + ", module:" + module + ", cause: " + String.join(",", errorMessages));
        this.name = name;
        this.module = module;
        this.errorMessages = errorMessages;
    }

    public static String convertThrowableMessage(final Throwable e) {

        final StringBuilder sb = new StringBuilder();
        if(e.getMessage() != null) {
            sb.append(e.getMessage());
            sb.append("\n");
        } else if(e.getLocalizedMessage() != null) {
            sb.append(e.getLocalizedMessage());
            sb.append("\n");
        }
        if(e.getCause() != null) {
            sb.append(e.getCause());
            sb.append("\n");
        }
        for(final StackTraceElement stackTrace : e.getStackTrace()) {
            sb.append(stackTrace.toString());
            sb.append("\n");
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return String.format("%s %s %s", module, name, errorMessages);
    }
}
