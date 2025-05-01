package com.mercari.solution.util.pipeline.action;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.gcp.BigQueryUtil;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class BigQueryAction implements Action {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryAction.class);

    public static class Parameters implements Serializable {

        public Op op;

        public String projectId;

        // for query job
        public String query;
        public Boolean useLegacySql;
        public BigQueryIO.TypedRead.QueryPriority priority;

        // for load job
        private List<String> sourceUris;

        // common
        private String destinationTable;
        private BigQueryIO.Write.WriteDisposition writeDisposition;
        private BigQueryIO.Write.CreateDisposition createDisposition;


        public Boolean wait;
        private String quotaUser;


        public Op getOp() {
            return op;
        }

        public String getProjectId() {
            return projectId;
        }

        public String getQuery() {
            return query;
        }

        public Boolean getUseLegacySql() {
            return useLegacySql;
        }

        public BigQueryIO.TypedRead.QueryPriority getPriority() {
            return priority;
        }

        public List<String> getSourceUris() {
            return sourceUris;
        }

        public String getDestinationTable() {
            return destinationTable;
        }

        public BigQueryIO.Write.WriteDisposition getWriteDisposition() {
            return writeDisposition;
        }

        public BigQueryIO.Write.CreateDisposition getCreateDisposition() {
            return createDisposition;
        }

        public Boolean getWait() {
            return wait;
        }

        public String getQuotaUser() {
            return quotaUser;
        }

        public List<String> validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.op == null) {
                errorMessages.add("action[" + name + "].bigquery.op must not be null");
            } else {
                switch (this.op) {
                    case query -> {
                        if(this.query == null) {
                            errorMessages.add("action[" + name + "].bigquery.query must not be null");
                        }
                    }
                    case load -> {
                        if(this.sourceUris == null || this.sourceUris.isEmpty()) {
                            errorMessages.add("action[" + name + "].bigquery.sourceUris must not be empty");
                        }
                        if(this.destinationTable == null) {
                            errorMessages.add("action[" + name + "].bigquery.destinationTable must not be null");
                        }
                    }
                    case extract -> {

                    }
                    case copy -> {

                    }
                }
            }
            return errorMessages;
        }

        public void setDefaults(final PipelineOptions options, final String config) {
            if(this.projectId == null) {
                this.projectId = DataflowOptions.getProject(options);
            }
            if(this.wait == null) {
                this.wait = true;
            }
            switch (this.op) {
                case query -> {
                    if(this.useLegacySql == null) {
                        this.useLegacySql = false;
                    }
                    if(this.priority == null) {
                        this.priority = BigQueryIO.TypedRead.QueryPriority.INTERACTIVE;
                    }
                }
            }
        }
    }

    enum Op {
        query,
        load,
        extract,
        copy
    }

    private final BigQueryAction.Parameters parameters;


    public static BigQueryAction of(final BigQueryAction.Parameters parameters) {
        return new BigQueryAction(parameters);
    }

    public BigQueryAction(final BigQueryAction.Parameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public Schema getOutputSchema() {
        return null;
    }

    @Override
    public void setup() {

    }

    @Override
    public MElement action() {
        return action(parameters);
    }

    @Override
    public MElement action(final MElement unionValue) {
        return action(parameters);
    }

    private static MElement action(final Parameters parameters) {
        switch (parameters.getOp()) {
            case query -> query(parameters);
            case load -> load(parameters);
            default -> throw new IllegalArgumentException();
        }
        return null;
    }

    public static void query(final BigQueryAction.Parameters parameters) {
        final Bigquery bigquery = BigQueryUtil.getBigquery();

        final JobConfigurationQuery jobConfigurationQuery = new JobConfigurationQuery();
        jobConfigurationQuery.setQuery(parameters.getQuery());
        jobConfigurationQuery.setPriority(parameters.getPriority().name());
        jobConfigurationQuery.setUseLegacySql(parameters.getUseLegacySql());
        if(parameters.getDestinationTable() != null) {
            jobConfigurationQuery.setDestinationTable(BigQueryUtil.getTableReference(parameters.getDestinationTable(), parameters.getProjectId()));
        }
        if(parameters.getCreateDisposition() != null) {
            jobConfigurationQuery.setCreateDisposition(parameters.getCreateDisposition().name());
        }
        if(parameters.getWriteDisposition() != null) {
            jobConfigurationQuery.setWriteDisposition(parameters.getWriteDisposition().name());
        }

        final Job request = new Job().setConfiguration(new JobConfiguration()
                .setQuery(jobConfigurationQuery)
                .setJobType("QUERY"));

        try {
            final Bigquery.Jobs.Insert insert = bigquery.jobs().insert(parameters.getProjectId(), request);
            if(parameters.getQuotaUser() != null) {
                insert.setQuotaUser(parameters.getQuotaUser());
            }
            Job response = insert.execute();
            if(parameters.getWait()) {
                waitJob(bigquery, response);
            }
            LOG.info("BigQuery job response: {}", response);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to execute query: " + parameters.getQuery(), e);
        }
    }

    public static void load(final BigQueryAction.Parameters parameters) {
        final Bigquery bigquery = BigQueryUtil.getBigquery();

        final JobConfigurationLoad jobConfigurationLoad = new JobConfigurationLoad();
        jobConfigurationLoad
                .setSourceUris(parameters.getSourceUris())
                .setDestinationTable(BigQueryUtil.getTableReference(parameters.getDestinationTable(), parameters.getProjectId()));
        if(parameters.getCreateDisposition() != null) {
            jobConfigurationLoad.setCreateDisposition(parameters.getCreateDisposition().name());
        }
        if(parameters.getWriteDisposition() != null) {
            jobConfigurationLoad.setWriteDisposition(parameters.getWriteDisposition().name());
        }

        final Job request = new Job().setConfiguration(new JobConfiguration()
                .setLoad(jobConfigurationLoad)
                .setJobType("LOAD"));

        try {
            final Bigquery.Jobs.Insert insert = bigquery.jobs().insert(parameters.getProjectId(), request);
            if(parameters.getQuotaUser() != null) {
                insert.setQuotaUser(parameters.getQuotaUser());
            }
            Job response = insert.execute();
            waitJob(bigquery, response);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to execute query: " + parameters.getQuery(), e);
        }
    }

    private static void waitJob(final Bigquery bigquery, Job response) throws IOException, InterruptedException {
        String state = response.getStatus().getState();
        long seconds = 0;
        while (!"DONE".equals(state)) {
            Thread.sleep(10000L);
            response = bigquery.jobs().get(response.getJobReference().getProjectId(), response.getJobReference().getJobId()).execute();
            state = response.getStatus().getState();
            seconds += 10;
            LOG.info("waiting jobId: {} for {} seconds.", response.getJobReference().getJobId(), seconds);
            System.out.println(seconds);
        }
        LOG.info("finished jobId: {} took {} seconds.", response.getJobReference().getJobId(), seconds);
        System.out.println(response.getJobReference());
    }

}
