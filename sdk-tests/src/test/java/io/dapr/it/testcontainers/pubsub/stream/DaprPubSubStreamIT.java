/*
 * Copyright 2025 The Dapr Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
limitations under the License.
*/

package io.dapr.it.testcontainers.pubsub.stream;

import io.dapr.client.DaprClient;
import io.dapr.client.DaprPreviewClient;
import io.dapr.client.SubscriptionListener;
import io.dapr.client.domain.CloudEvent;
import io.dapr.it.testcontainers.DaprClientFactory;
import io.dapr.testcontainers.DaprContainer;
import io.dapr.testcontainers.DaprLogLevel;
import io.dapr.utils.TypeRef;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static io.dapr.it.Retry.callWithRetry;
import static io.dapr.it.testcontainers.ContainerConstants.DAPR_RUNTIME_IMAGE_TAG;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * PubSub Streaming Integration Test using Testcontainers.
 * 
 * This test validates the streaming subscription functionality of Dapr's pubsub
 * using the DaprPreviewClient's subscribeToEvents method.
 */
@Testcontainers
@Tag("testcontainers")
public class DaprPubSubStreamIT {

  private static final Logger LOG = LoggerFactory.getLogger(DaprPubSubStreamIT.class);

  // Must be a large enough number, so we validate that we get more than the initial batch
  // sent by the runtime. When this was first added, the batch size in runtime was set to 10.
  private static final int NUM_MESSAGES = 100;
  private static final String TOPIC_NAME = "stream-topic";
  private static final String PUBSUB_NAME = "pubsub";

  private static final Network DAPR_NETWORK = Network.newNetwork();

  @Container
  private static final DaprContainer DAPR_CONTAINER = new DaprContainer(DAPR_RUNTIME_IMAGE_TAG)
      .withAppName("pubsub-stream-dapr-app")
      .withNetwork(DAPR_NETWORK)
      .withDaprLogLevel(DaprLogLevel.DEBUG)
      .withLogConsumer(outputFrame -> LOG.info(outputFrame.getUtf8String()));

  @Test
  public void testPubSubStream() throws Exception {
    var runId = UUID.randomUUID().toString();

    try (DaprClient client = DaprClientFactory.createDaprClientBuilder(DAPR_CONTAINER).build();
         DaprPreviewClient previewClient = DaprClientFactory.createDaprClientBuilder(DAPR_CONTAINER)
             .buildPreviewClient()) {

      // Publish messages
      for (int i = 0; i < NUM_MESSAGES; i++) {
        String message = String.format("This is message #%d on topic %s for run %s", i, TOPIC_NAME, runId);
        client.publishEvent(PUBSUB_NAME, TOPIC_NAME, message).block();
        LOG.info("Published message: '{}' to topic '{}' pubsub_name '{}'", message, TOPIC_NAME, PUBSUB_NAME);
      }

      LOG.info("Starting subscription for {}", TOPIC_NAME);

      Set<String> messages = Collections.synchronizedSet(new HashSet<>());
      Set<String> errors = Collections.synchronizedSet(new HashSet<>());

      var random = new Random(37);  // predictable random.
      var listener = new SubscriptionListener<String>() {
        @Override
        public Mono<Status> onEvent(CloudEvent<String> event) {
          return Mono.fromCallable(() -> {
            // Useful to avoid false negatives running locally multiple times.
            if (event.getData().contains(runId)) {
              // 5% failure rate.
              var decision = random.nextInt(100);
              if (decision < 5) {
                if (decision % 2 == 0) {
                  throw new RuntimeException("artificial exception on message " + event.getId());
                }
                return Status.RETRY;
              }

              messages.add(event.getId());
              return Status.SUCCESS;
            }

            return Status.DROP;
          });
        }

        @Override
        public void onError(RuntimeException exception) {
          errors.add(exception.getMessage());
        }
      };

      try (var subscription = previewClient.subscribeToEvents(PUBSUB_NAME, TOPIC_NAME, listener, TypeRef.STRING)) {
        callWithRetry(() -> {
          var messageCount = messages.size();
          LOG.info("Got {} messages out of {} for topic {}.", messageCount, NUM_MESSAGES, TOPIC_NAME);
          assertEquals(NUM_MESSAGES, messages.size());
          assertEquals(4, errors.size());
        }, 120000); // Time for runtime to retry messages.

        subscription.close();
        subscription.awaitTermination();
      }
    }
  }
}
