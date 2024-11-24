package com.mercari.solution.util.pipeline.action;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class DataflowAction implements Action {

    private static final Logger LOG = LoggerFactory.getLogger(DataflowAction.class);

    public static class Parameters implements Serializable {

        private Op op;

        private DataflowOptions options;

        public List<String> validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.op == null) {
                errorMessages.add("action[" + name + "].dataflow.op must not be null");
            } else {
                switch (this.op) {
                    case launchTemplate, launchFlexTemplate -> {

                    }
                }
            }
            return errorMessages;
        }

        public void setDefaults(final MPipeline.MPipelineOptions options) {
            DataflowOptions copy = DataflowOptions.copy(options);
            /*
            if(this.project == null) {
                this.project = dataflowPipelineOptions.getProject();
            }
            if(this.region == null) {
                this.region = dataflowPipelineOptions.getRegion();
            }
            if(this.templateLocation == null) {
                this.templateLocation = dataflowPipelineOptions.getTemplateLocation();
            }
            if(this.tempLocation == null) {
                this.tempLocation = options.getTempLocation();
            }
            if(this.stagingLocation == null) {
                this.stagingLocation = dataflowPipelineOptions.getStagingLocation();
            }
            if(this.workerMachineType == null) {
                this.workerMachineType = dataflowPipelineOptions.getWorkerMachineType();
            }
            if(this.maxNumWorkers == null) {
                this.maxNumWorkers = dataflowPipelineOptions.getMaxNumWorkers();
            }
            if(this.subnetwork == null) {
                this.subnetwork = dataflowPipelineOptions.getSubnetwork();
            }
            if(this.serviceAccount == null) {
                this.serviceAccount = dataflowPipelineOptions.getServiceAccount();
            }
            if(this.parameters == null && config != null) {
                this.parameters = new HashMap<>();
                this.parameters.put("config", config);
            }
            if(this.validateOnly == null) {
                this.validateOnly = false;
            }

             */
        }

    }

    enum Op {
        launchTemplate,
        launchFlexTemplate;
    }

    private final Parameters parameters;
    private final Map<String, String> labels;


    public static DataflowAction of(final Parameters parameters, final Map<String, String> labels) {
        return new DataflowAction(parameters, labels);
    }

    public DataflowAction(final Parameters parameters, final Map<String, String> labels) {
        this.parameters = parameters;
        this.labels = labels;
    }

    @Override
    public Schema getOutputSchema() {
        return switch (this.parameters.op) {
            case launchTemplate -> throw new RuntimeException();
            case launchFlexTemplate -> Schema.builder()
                    .withField(Schema.Field.of("jobId", Schema.FieldType.STRING.withNullable(true)))
                    .build();
        };
    }

    @Override
    public void setup() {

    }

    @Override
    public MElement action() {
        return action(parameters, labels);
    }


    @Override
    public MElement action(final MElement unionValue) {
        return action(parameters, labels);
    }

    private static MElement action(final Parameters parameters, final Map<String, String> labels) {
        try {
            switch (parameters.op) {
                case launchTemplate -> dataflowLaunchTemplate(parameters);
                case launchFlexTemplate -> dataflowLaunchFlexTemplate(parameters, labels);
                default -> throw new IllegalArgumentException();
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void dataflowLaunchTemplate(final Parameters parameters) throws IOException {
        throw new NotImplementedException("dataflowLaunchTemplate");
    }

    private static void dataflowLaunchFlexTemplate(final Parameters dataflowParameters, final Map<String, String> labels) throws IOException {

        /*
        final LaunchFlexTemplateParameter parameter = LaunchFlexTemplateParameter
                .newBuilder()
                .setContainerSpecGcsPath(dataflowParameters.getTemplateLocation())
                .putAllParameters()
                .setEnvironment(FlexTemplateRuntimeEnvironment.newBuilder()
                        .build())
                .build();
        parameter.setContainerSpecGcsPath(dataflowParameters.getTemplateLocation());
        parameter.setParameters(dataflowParameters.getParameters());

        final String jobName = Optional
                .ofNullable(dataflowParameters.getJobName())
                .orElseGet(() -> "dataflow-flex-template-job-" + Instant.now().toEpochMilli());
        parameter.setJobName(jobName);

        if(dataflowParameters.getUpdate() != null) {
            parameter.setUpdate(dataflowParameters.getUpdate());
        }
        if(dataflowParameters.getLaunchOptions() != null) {
            parameter.setLaunchOptions(dataflowParameters.getLaunchOptions());
        }

        final FlexTemplateRuntimeEnvironment environment = new FlexTemplateRuntimeEnvironment();
        if(dataflowParameters.getTempLocation() != null) {
            environment.setTempLocation(dataflowParameters.getTempLocation());
        }
        if(dataflowParameters.getStagingLocation() != null) {
            environment.setStagingLocation(dataflowParameters.getStagingLocation());
        }
        if(dataflowParameters.getSubnetwork() != null) {
            environment.setSubnetwork(dataflowParameters.getSubnetwork());
        }
        if(dataflowParameters.getServiceAccount() != null) {
            environment.setServiceAccountEmail(dataflowParameters.getServiceAccount());
        }
        if(dataflowParameters.getWorkerMachineType() != null) {
            environment.setMachineType(dataflowParameters.getWorkerMachineType());
        }
        if(dataflowParameters.getMaxNumWorkers() != null) {
            environment.setMaxWorkers(dataflowParameters.getMaxNumWorkers());
        }
        if(labels != null) {
            environment.setAdditionalUserLabels(labels);
        }

        if(!environment.isEmpty()) {
            parameter.setEnvironment(environment);
        }

        final LaunchFlexTemplateResponse response = DataflowUtil.launchFlexTemplate(
                dataflowParameters.getProject(),
                dataflowParameters.getRegion(),
                parameter,
                dataflowParameters.getValidateOnly());
        LOG.info("LaunchFlexTemplate: {}", response.toString());

         */
    }

}
