package com.mercari.solution.config.options;

import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

public class BeamSQLOptions implements Serializable {

    private String plannerName;
    private String zetaSqlDefaultTimezone;
    private Boolean verifyRowValues;

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final BeamSQLOptions beamsql) {

        //pipelineOptions.setPlannerName("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner");
        //pipelineOptions.setPlannerName("org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner");

        if(beamsql == null) {
            return;
        }

        try {
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>)Class.forName("org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions");
            final PipelineOptions beamsqlOptions = pipelineOptions.as(clazz);

            if(beamsql.plannerName != null) {
                clazz.getMethod("setPlannerName", String.class).invoke(beamsqlOptions, beamsql.plannerName);
            }
            if(beamsql.zetaSqlDefaultTimezone != null) {
                clazz.getMethod("setZetaSqlDefaultTimezone", String.class).invoke(beamsqlOptions, beamsql.zetaSqlDefaultTimezone);
            }
            if(beamsql.verifyRowValues != null) {
                clazz.getMethod("setVerifyRowValues", Boolean.class).invoke(beamsqlOptions, beamsql.verifyRowValues);
            }

        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to set dataflow runner pipeline options", e);
        }

    }

}
