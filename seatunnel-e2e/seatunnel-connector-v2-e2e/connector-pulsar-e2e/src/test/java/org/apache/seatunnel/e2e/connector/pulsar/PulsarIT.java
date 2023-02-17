package org.apache.seatunnel.e2e.connector.pulsar;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class PulsarIT extends TestSuiteBase implements TestResource {
    private static final String PULSAR_IMAGE_NAME = "apachepulsar/pulsar:2.10.0";
    private static final Integer PULSAR_BROKER_PORT = 6650;
    private static final Integer PULSAR_BROKER_HTTP_PORT = 8080;
    public static final String PULSAR_HOST = "pulsar_e2e";
    private PulsarContainer pulsarContainer;


    @Override
    @BeforeAll
    public void startUp() throws Exception {
        pulsarContainer = new PulsarContainer(DockerImageName.parse(PULSAR_IMAGE_NAME))
            .withNetwork(NETWORK)
            .withNetworkAliases(PULSAR_HOST)
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(PULSAR_IMAGE_NAME)));
        pulsarContainer.setPortBindings(Lists.newArrayList(
            String.format("%s:%s", PULSAR_BROKER_PORT, PULSAR_BROKER_PORT),
            String.format("%s:%s", PULSAR_BROKER_HTTP_PORT, PULSAR_BROKER_HTTP_PORT)
        ));
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
    }

    @Test
    void testPull() {
        System.out.println(pulsarContainer.getHttpServiceUrl());
        System.out.println(pulsarContainer.getPulsarBrokerUrl());
    }

    private void initTopic() {

    }
}
