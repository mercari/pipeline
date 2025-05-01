package com.mercari.solution.config.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;
import java.util.List;

public class GCPOptions implements Serializable {

    private String project;
    private String impersonateServiceAccount;
    private String workerRegion;
    private String workerZone;
    private Boolean enableStreamingEngine;
    private String dataflowKmsKey;
    private List<String> gcpOauthScopes;

    private PubsubOptions pubsub;
    private BigQueryOptions bigquery;
    private FirestoreOptions firestore;


    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final GCPOptions gcp) {

        if(gcp == null) {
            return;
        }

        if(gcp.project != null) {
            pipelineOptions.as(GcpOptions.class).setProject(gcp.project);
        }
        if(gcp.workerRegion != null) {
            pipelineOptions.as(GcpOptions.class).setWorkerRegion(gcp.workerRegion);
        }
        if(gcp.workerZone != null) {
            pipelineOptions.as(GcpOptions.class).setWorkerZone(gcp.workerZone);
        }
        if(gcp.enableStreamingEngine != null) {
            pipelineOptions.as(GcpOptions.class).setEnableStreamingEngine(gcp.enableStreamingEngine);
        }
        if(gcp.impersonateServiceAccount != null) {
            pipelineOptions.as(GcpOptions.class).setImpersonateServiceAccount(gcp.impersonateServiceAccount);
        }
        if(gcp.dataflowKmsKey != null) {
            pipelineOptions.as(GcpOptions.class).setDataflowKmsKey(gcp.dataflowKmsKey);
        }
        if(gcp.gcpOauthScopes != null && !gcp.gcpOauthScopes.isEmpty()) {
            pipelineOptions.as(GcpOptions.class).setGcpOauthScopes(gcp.gcpOauthScopes);
        }

        PubsubOptions.setOptions(pipelineOptions, gcp.pubsub);
        BigQueryOptions.setOptions(pipelineOptions, gcp.bigquery);
        FirestoreOptions.setOptions(pipelineOptions, gcp.firestore);
    }

    public static class PubsubOptions implements Serializable {

        private String pubsubRootUrl;

        private static void setOptions(
                final PipelineOptions pipelineOptions,
                final PubsubOptions pubsub) {

            if(pubsub == null) {
                return;
            }

            if(pubsub.pubsubRootUrl != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions.class).setPubsubRootUrl(pubsub.pubsubRootUrl);
            }
        }

    }

    public static class BigQueryOptions implements Serializable {

        private String bigqueryProject;
        private String bigqueryEndpoint;
        private Boolean enableStorageReadApiV2;
        private Boolean useStorageApiConnectionPool;

        private static void setOptions(
                final PipelineOptions pipelineOptions,
                final BigQueryOptions bigquery) {

            if(bigquery == null) {
                return;
            }

            if(bigquery.bigqueryProject != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions.class).setBigQueryProject(bigquery.bigqueryProject);
            }
            if(bigquery.bigqueryEndpoint != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions.class).setBigQueryEndpoint(bigquery.bigqueryEndpoint);
            }
            if(bigquery.enableStorageReadApiV2 != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions.class).setEnableStorageReadApiV2(bigquery.enableStorageReadApiV2);
            }
            if(bigquery.useStorageApiConnectionPool != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions.class).setUseStorageApiConnectionPool(bigquery.useStorageApiConnectionPool);
            }
        }

    }

    public static class FirestoreOptions implements Serializable {

        private String firestoreProject;
        private String firestoreHost;
        private String emulatorHost;
        private String firestoreDb;

        private static void setOptions(
                final PipelineOptions pipelineOptions,
                final FirestoreOptions firestore) {

            if(firestore == null) {
                return;
            }

            if(firestore.firestoreProject != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions.class).setFirestoreProject(firestore.firestoreProject);
            }
            if(firestore.firestoreHost != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions.class).setFirestoreHost(firestore.firestoreHost);
            }
            if(firestore.emulatorHost != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions.class).setEmulatorHost(firestore.emulatorHost);
            }
            if(firestore.firestoreDb != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions.class).setFirestoreDb(firestore.firestoreDb);
            }
        }

    }

}

