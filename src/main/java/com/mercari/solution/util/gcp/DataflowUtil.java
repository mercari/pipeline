package com.mercari.solution.util.gcp;

import com.google.dataflow.v1beta3.*;

import java.io.IOException;

public class DataflowUtil {

    public static LaunchFlexTemplateResponse launchFlexTemplate(
            final String project,
            final String region,
            final LaunchFlexTemplateParameter parameter,
            final Boolean validateOnly) throws IOException {

        final LaunchFlexTemplateRequest request = LaunchFlexTemplateRequest
                .newBuilder()
                .setProjectId(project)
                .setLocation(region)
                .setLaunchParameter(parameter)
                .setValidateOnly(validateOnly)
                .build();

        try(final FlexTemplatesServiceClient client = FlexTemplatesServiceClient.create()) {
            final LaunchFlexTemplateResponse response = client.launchFlexTemplate(request);
            return response;
        }
    }

    public static Job update(
            final String project,
            final String region,
            final String jobId,
            final JobState requestedState) throws IOException {

        final UpdateJobRequest request = UpdateJobRequest.newBuilder()
                .setProjectId(project)
                .setLocation(region)
                .setJobId(jobId)
                .setJob(Job.newBuilder()
                        .setRequestedState(requestedState)
                        .build())
                .build();

        //JOB_STATE_CANCELLED
        try(final JobsV1Beta3Client client = JobsV1Beta3Client.create()) {
            return client.updateJob(request);
        }
    }

    public static Job cancel(
            final String project,
            final String region,
            final String jobId) throws IOException {

        return update(project, region, jobId, JobState.JOB_STATE_CANCELLED);
    }

    public static LaunchTemplateResponse launchTemplate(
            final String project,
            final String region,
            final String gcsPath,
            final LaunchTemplateParameters parameter,
            final Boolean validateOnly) throws IOException {

        final LaunchTemplateRequest request = LaunchTemplateRequest
                .newBuilder()
                .setProjectId(project)
                .setLocation(region)
                .setGcsPath(gcsPath)
                .setLaunchParameters(parameter)
                .setValidateOnly(validateOnly)
                .build();

        try(final TemplatesServiceClient client = TemplatesServiceClient.create()) {
            return client.launchTemplate(request);
        }
    }

}
