package com.mercari.solution.config.options;

import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.io.Serializable;

public class AWSOptions implements Serializable {

    private String region;

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final AWSOptions aws) {

        if(aws == null) {
            return;
        }

        final AwsOptions awsOptions = pipelineOptions.as(AwsOptions.class);
        if(aws.region != null) {
            awsOptions.setAwsRegion(Region.of(aws.region));
        }
        AwsCredentialsProvider provider = new AwsCredentialsProvider() {
            @Override
            public AwsCredentials resolveCredentials() {
                return null;
            }
        };
        //awsOptions.setAwsCredentialsProvider();

    }

}
