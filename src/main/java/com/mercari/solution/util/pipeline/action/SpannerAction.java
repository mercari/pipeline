package com.mercari.solution.util.pipeline.action;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.*;
import com.google.spanner.admin.instance.v1.UpdateInstanceMetadata;
import com.mercari.solution.util.gcp.SpannerUtil;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SpannerAction {

    private static class TablePrepareDoFn extends DoFn<List<String>, String> {

        private static final Logger LOG = LoggerFactory.getLogger(TablePrepareDoFn.class);

        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final boolean emulator;

        TablePrepareDoFn(final String projectId, final String instanceId, final String databaseId, final Boolean emulator) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.emulator = emulator;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final List<String> ddl = c.element();
            if(ddl == null) {
                return;
            }
            if(ddl.isEmpty()) {
                c.output("ok");
                return;
            }
            try(final Spanner spanner = SpannerUtil.connectSpanner(projectId, 1, 1, 1, false, emulator)) {
                for(String sql : ddl) {
                    LOG.info("Execute DDL: {}", sql);
                    SpannerUtil.executeDdl(spanner, instanceId, databaseId, sql);
                }
                c.output("ok");
            }
        }
    }


    private static class TableEmptyDoFn<InputT> extends DoFn<InputT, String> {

        private static final Logger LOG = LoggerFactory.getLogger(TableEmptyDoFn.class);

        private final String projectId;
        private final String instanceId;
        private final String databaseId;
        private final String table;
        private final boolean emulator;

        TableEmptyDoFn(final String projectId, final String instanceId, final String databaseId, final String table, final Boolean emulator) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
            this.table = table;
            this.emulator = emulator;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            try(final Spanner spanner = SpannerUtil
                    .connectSpanner(projectId, 1, 1, 1, false, emulator)) {

                final long result = SpannerUtil.emptyTable(spanner, projectId, instanceId, databaseId, table);
                c.output("ok");
            }
        }
    }

    private static class SpannerScaleDoFn extends DoFn<Integer, Integer> {

        private static final Logger LOG = LoggerFactory.getLogger(SpannerScaleDoFn.class);

        private final String projectId;
        private final String instanceId;
        private final Integer rebalancingMinite;

        public SpannerScaleDoFn(
                final String projectId,
                final String instanceId,
                final Integer rebalancingMinite) {

            this.projectId = projectId;
            this.instanceId = instanceId;
            this.rebalancingMinite = rebalancingMinite;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            final Integer nodeCount = c.element();
            if(nodeCount == null || nodeCount <= 0) {
                LOG.info("Not scale spanner instance");
                c.output(-1);
                return;
            }
            final SpannerOptions options = SpannerOptions.newBuilder()
                    .setProjectId(this.projectId)
                    .build();

            final InstanceId instanceId = InstanceId.of(this.projectId, this.instanceId);
            try(final Spanner spanner = options.getService()) {
                final Instance instance = spanner
                        .getInstanceAdminClient().getInstance(instanceId.getInstance());
                final int currentNodeCount = instance.getNodeCount();
                LOG.info("Current spanner instance: {} nodeCount: {}", instanceId.getInstance(), currentNodeCount);
                if(currentNodeCount == nodeCount) {
                    LOG.info("Same spanner instance current and required node count.");
                    c.output(-1);
                    return;
                }

                final OperationFuture<Instance, UpdateInstanceMetadata> meta = spanner
                        .getInstanceAdminClient()
                        .updateInstance(InstanceInfo
                                .newBuilder(instanceId)
                                .setDisplayName(instance.getDisplayName())
                                .setNodeCount(nodeCount)
                                .build());

                meta.get();
                int waitingSeconds = 0;
                while(!meta.isDone()) {
                    Thread.sleep(5 * 1000L);
                    LOG.info("waiting scaling...");
                    waitingSeconds += 5;
                    if(waitingSeconds > 3600) {
                        throw new IllegalArgumentException("DEADLINE EXCEEDED for scale spanner instance!");
                    }
                }
                LOG.info("Scale spanner instance: " + instanceId.getInstance() + " nodeCount: " + nodeCount);

                int waitingMinutes = 0;
                while(waitingMinutes < rebalancingMinite) {
                    Thread.sleep(60 * 1000L);
                    LOG.info("waiting rebalancing minute: " + waitingMinutes + "/" + rebalancingMinite);
                    waitingMinutes += 1;
                }
                LOG.info("finished waiting.");
                c.output(currentNodeCount);
            }
        }
    }

}
