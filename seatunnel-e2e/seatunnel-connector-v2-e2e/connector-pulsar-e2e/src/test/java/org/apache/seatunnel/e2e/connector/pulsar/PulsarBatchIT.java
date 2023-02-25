/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.pulsar;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class PulsarBatchIT extends TestSuiteBase implements TestResource {

    private static final String PULSAR_IMAGE_NAME = "apachepulsar/pulsar:2.3.0";
    private static final Integer PULSAR_BROKER_PORT = 6650;
    private static final Integer PULSAR_BROKER_HTTP_PORT = 8080;
    public static final String PULSAR_HOST = "pulsar.e2e";
    public static final String TOPIC = "topic-it";
    private PulsarContainer pulsarContainer;
    private PulsarClient client;
    private Producer<String> producer;
    public static int rowNum = 100;

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        pulsarContainer =
                new PulsarContainer(DockerImageName.parse(PULSAR_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(PULSAR_HOST)
                        .withAccessToHost(true)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(PULSAR_IMAGE_NAME)));
        pulsarContainer.setPortBindings(
                Lists.newArrayList(
                        String.format("%s:%s", PULSAR_BROKER_PORT, PULSAR_BROKER_PORT),
                        String.format("%s:%s", PULSAR_BROKER_HTTP_PORT, PULSAR_BROKER_HTTP_PORT)));

        Startables.deepStart(Stream.of(pulsarContainer)).join();
        Awaitility.given()
                .ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initTopic);
    }

    @Override
    public void tearDown() throws Exception {
        pulsarContainer.close();
        client.close();
        producer.close();
    }

    private void initTopic() throws PulsarClientException {
        client = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();
        producer = client.newProducer(Schema.STRING).topic(TOPIC).create();
        produceData();
    }

    private void produceData() {
        try {
            while (rowNum > 0) {
                if (producer.isConnected()) {
                    producer.send(UUID.randomUUID().toString());
                    rowNum--;
                }
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @TestTemplate
    void testPulsarBatch(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/batch_pulsar_to_console.conf");
        Assertions.assertEquals(execResult.getExitCode(), 0);
    }
}
