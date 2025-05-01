package com.mercari.solution.util.gcp;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.*;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.collect.ImmutableList;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.regex.Pattern;

public class PubSubUtil {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubUtil.class);

    private static final Pattern PATTERN_TOPIC = Pattern.compile("^projects\\/[a-zA-Z0-9_-]+\\/topics\\/[a-zA-Z0-9_-]+$");
    private static final Pattern PATTERN_SUBSCRIPTION = Pattern.compile("^projects\\/[a-zA-Z0-9_-]+\\/subscriptions\\/[.a-zA-Z0-9_-]+$");
    private static final Pattern PATTERN_SNAPSHOT = Pattern.compile("^projects\\/[a-zA-Z0-9_-]+\\/snapshots\\/[a-zA-Z0-9_-]+$");

    public static Pubsub pubsub(final String rootUrl) {
        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
        try {
            final Credentials credential = GoogleCredentials.getApplicationDefault();
            final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(credential),
                    // Do not log 404. It clutters the output and is possibly even required by the caller.
                    new RetryHttpRequestInitializer(ImmutableList.of(404)));

            final Pubsub.Builder builder = new Pubsub.Builder(transport, jsonFactory, initializer)
                    .setApplicationName("PubSubClient");
            if(rootUrl == null) {
                return builder.build();
            } else {
                return builder.setRootUrl(rootUrl).build();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Pubsub pubsub() {
        return pubsub(null);
    }

    public static Schema getSchemaFromTopic(final String topic) {
        return getSchemaFromTopic(pubsub(), topic);
    }

    public static Schema getSchemaFromTopic(final Pubsub pubsub, final String topicResource) {
        try {
            final Topic topic = pubsub.projects().topics().get(topicResource).execute();
            if(topic.getSchemaSettings() == null || topic.getSchemaSettings().getSchema() == null) {
                throw new IllegalArgumentException("Unable to get schema from topic: " + topicResource);
            }
            final String schemaResource = topic.getSchemaSettings().getSchema();
            final com.google.api.services.pubsub.model.Schema topicSchema = pubsub.projects().schemas().get(schemaResource).execute();
            return switch (topicSchema.getType().toLowerCase()) {
                case "avro" -> AvroSchemaUtil.convertSchema(topicSchema.getDefinition());
                case "protocol-buffer" -> throw new IllegalArgumentException("Not supported protobuf yet");
                default -> throw new IllegalArgumentException("Not supported topic schema type: " + topicSchema.getType());
            };
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to get schema for topic: " + topicResource, e);
        }
    }

    public static Schema getSchemaFromSubscription(final String subscriptionResource) {
        final Pubsub pubsub = pubsub();
        return getSchemaFromSubscription(pubsub, subscriptionResource);
    }

    public static Schema getSchemaFromSubscription(final Pubsub pubsub, final String subscriptionResource) {
        try {
            final Subscription subscription = pubsub.projects().subscriptions().get(subscriptionResource).execute();
            final String topicResource = subscription.getTopic();
            return getSchemaFromTopic(pubsub, topicResource);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to get schema for subscription: " + subscriptionResource, e);
        }
    }

    public static String getDeadLetterTopic(String subscriptionResource) {
        return getDeadLetterTopic(pubsub(), subscriptionResource);
    }

    public static String getDeadLetterTopic(final Pubsub pubsub, String subscriptionResource) {
        try {
            final Subscription subscription = pubsub
                    .projects()
                    .subscriptions()
                    .get(subscriptionResource)
                    .execute();
            if (subscription.getDeadLetterPolicy() == null) {
                return null;
            }
            return subscription.getDeadLetterPolicy().getDeadLetterTopic();
        } catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == 404) {
                return null;
            } else if(e.getStatusCode() == 403) {
                LOG.warn("dataflow worker does not have dead-letter topic access permission for subscription: " + subscriptionResource);
                return null;
            }
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isTopicResource(final String name) {
        return PATTERN_TOPIC.matcher(name).find();
    }

    public static boolean isSubscriptionResource(final String name) {
        return PATTERN_SUBSCRIPTION.matcher(name).find();
    }

    public static boolean isSnapshotResource(final String name) {
        return PATTERN_SNAPSHOT.matcher(name).find();
    }

    public static List<String> publish(
            final String topic,
            final List<PubsubMessage> messages) throws IOException {

        return publish(pubsub(), topic, messages);
    }

    public static List<String> publish(
            final Pubsub pubsub,
            final String topic,
            final List<PubsubMessage> messages) throws IOException {

        final PublishRequest request = new PublishRequest().setMessages(messages);
        final PublishResponse response =pubsub.projects().topics().publish(topic, request).execute();
        return response.getMessageIds();
    }

    public static List<ReceivedMessage> pull(
            final Pubsub pubsub,
            final String subscription,
            final int maxMessages,
            final boolean ack) throws IOException {

        final PullResponse response = pubsub
                .projects()
                .subscriptions()
                .pull(subscription, new PullRequest().setMaxMessages(maxMessages))
                .execute();

        if(ack && !response.isEmpty()) {
            final List<String> ackIds = response.getReceivedMessages()
                    .stream()
                    .map(ReceivedMessage::getAckId)
                    .toList();
            final Empty empty = pubsub
                    .projects()
                    .subscriptions()
                    .acknowledge(subscription, new AcknowledgeRequest().setAckIds(ackIds))
                    .execute();
        }

        return response.getReceivedMessages();
    }

    public static SeekResponse seek(
            final String subscription,
            final String time,
            final String snapshot) throws IOException {

        return seek(pubsub(), subscription, time, snapshot);
    }

    public static SeekResponse seek(
            final Pubsub pubsub,
            final String subscription,
            final String time,
            final String snapshot) throws IOException {

        if(time == null && snapshot == null) {
            throw new IllegalArgumentException("seek operation requires time or snapshot. both are null");
        }

        final SeekRequest seekRequest = new SeekRequest();
        if(time != null) {
            seekRequest.setTime(time);
        }
        if(snapshot != null) {
            if(!isSnapshotResource(snapshot)) {
                throw new IllegalArgumentException("seek.snapshot resource name is illegal: " + snapshot);
            }
            seekRequest.setSnapshot(snapshot);
        }

        return pubsub
                .projects()
                .subscriptions()
                .seek(subscription, seekRequest)
                .execute();
    }

    public static boolean existsSubscription(final String subscription) {
        return existsSubscription(pubsub(), subscription);
    }

    public static boolean existsSubscription(
            final Pubsub pubsub,
            final String subscription) {

        if(!isSubscriptionResource(subscription)) {
            throw new IllegalArgumentException("subscription resource name is illegal: " + subscription);
        }
        try {
            final Subscription s = pubsub.projects().subscriptions().get(subscription).execute();
            return true;
        } catch (GoogleJsonResponseException e) {
            if(e.getStatusCode() == 404) {
                return false;
            }
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Subscription createSubscription(
            final String topic,
            final String name) {

        return createSubscription(pubsub(), topic, name);
    }

    public static Subscription createSubscription(
            final Pubsub pubsub,
            final String topic,
            final String name) {

        if(!isTopicResource(topic)) {
            throw new IllegalArgumentException("topic resource name is illegal: " + topic);
        }
        if(!isSubscriptionResource(name)) {
            throw new IllegalArgumentException("subscription resource name is illegal: " + name);
        }

        final Subscription content = new Subscription();
        content.setTopic(topic);
        try {
            return pubsub
                    .projects()
                    .subscriptions()
                    .create(name, content)
                    .execute();
        } catch (final IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static Snapshot createSnapshot(
            final Pubsub pubsub,
            final String subscription,
            final String name) {

        if(!isSubscriptionResource(subscription)) {
            throw new IllegalArgumentException("subscription resource name is illegal: " + subscription);
        }
        if(!isSnapshotResource(name)) {
            throw new IllegalArgumentException("snapshot resource name is illegal: " + name);
        }

        final CreateSnapshotRequest request = new CreateSnapshotRequest();
        request.setSubscription(subscription);
        try {
            return pubsub
                    .projects()
                    .snapshots()
                    .create(name, request)
                    .execute();
        } catch (final IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static void deleteSubscription(final String subscription) {
        deleteSubscription(pubsub(), subscription);
    }

    public static void deleteSubscription(
            final Pubsub pubsub,
            final String subscription) {

        if(!isSubscriptionResource(subscription)) {
            throw new IllegalArgumentException("subscription resource name is illegal: " + subscription);
        }
        try {
            pubsub.projects().subscriptions().delete(subscription).execute();
        } catch (final IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static String getTextMessage(final String subscription) throws IOException {
        return getTextMessage(pubsub(), subscription);
    }

    public static String getTextMessage(
            final Pubsub pubsub,
            final String subscription) throws IOException {

        final PullResponse response = pubsub
                .projects()
                .subscriptions()
                .pull(subscription, new PullRequest()
                        .setMaxMessages(1))
                .execute();
        if(response.isEmpty() && (response.getReceivedMessages() == null || response.getReceivedMessages().isEmpty())) {
            LOG.info("response is empty: " + response.getReceivedMessages());
            return null;
        } else if(response.getReceivedMessages().size() > 1) {
            LOG.info("response is over zero: " + response.getReceivedMessages().size());
        }

        final ReceivedMessage receivedMessage = response.getReceivedMessages().get(0);
        final Empty empty = pubsub.projects().subscriptions()
                .acknowledge(subscription, new AcknowledgeRequest()
                        .setAckIds(List.of(receivedMessage.getAckId())))
                .execute();
        final String data = receivedMessage.getMessage().getData();
        return convertReceivedMessageData(data);
    }

    public static String convertPubsubMessageData(final String text) {
        return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }

    public static String convertReceivedMessageData(final String data) {
        return new String(Base64.getDecoder().decode(data), StandardCharsets.UTF_8);
    }

}
