package com.mercari.solution.util.pipeline.action.vertexai;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Job;
import com.google.auth.oauth2.AccessToken;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.gcp.IAMUtil;
import com.mercari.solution.util.gcp.vertexai.GeminiUtil;
import com.mercari.solution.util.pipeline.OptionUtil;
import com.mercari.solution.util.pipeline.action.Action;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.List;

public class GeminiAction implements Action {

    private static final Logger LOG = LoggerFactory.getLogger(GeminiAction.class);

    public static class Parameters implements Serializable {

        private Op op;
        private String project;
        private String region;
        private GeminiUtil.BatchPredictionJobsRequest batchPredictionJobsRequest;
        public Boolean wait;


        public Op getOp() {
            return op;
        }

        public List<String> validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(region == null) {
                errorMessages.add("action[" + name + "].vertexai.gemini.region must not be null");
            }
            if(this.op == null) {
                errorMessages.add("action[" + name + "].vertexai.gemini.op must not be null");
            } else {
                switch (this.op) {
                    case batchPrediction -> {
                        if(batchPredictionJobsRequest == null) {
                            errorMessages.add("action[" + name + "].vertexai.gemini.batchPredictionJobsRequest must not be null");
                        }
                    }
                }
            }
            return errorMessages;
        }

        public void setDefaults(final PipelineOptions options, final String config) {
            if(project == null) {
                project = OptionUtil.getDefaultProject();
            }
            if(this.wait == null) {
                this.wait = true;
            }
            switch (this.op) {
                case batchPrediction -> {
                    batchPredictionJobsRequest.setDefaults();
                }
            }
        }
    }

    enum Op {
        batchPrediction
    }

    private final Parameters parameters;


    public static GeminiAction of(final Parameters parameters) {
        return new GeminiAction(parameters);
    }

    public GeminiAction(final Parameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public Schema getOutputSchema() {
        return Schema.builder()
                .withField("body", Schema.FieldType.STRING)
                .build();
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
            case batchPrediction -> batchPredict(parameters);
            default -> throw new IllegalArgumentException("Illegal op: " + parameters.getOp());
        }
        return null;
    }

    public static void batchPredict(final Parameters parameters) {
        try(final HttpClient httpClient = HttpClient.newHttpClient()) {
            final AccessToken accessToken = IAMUtil.getAccessToken();
            final GeminiUtil.BatchPredictionJobsResponse response = GeminiUtil
                    .batchPredictionJobs(httpClient, accessToken.getTokenValue(), parameters.project, parameters.region, parameters.batchPredictionJobsRequest);
        } catch (IOException e) {
            throw new RuntimeException("Failed to vertexai.gemini.batchPrediction", e);
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
