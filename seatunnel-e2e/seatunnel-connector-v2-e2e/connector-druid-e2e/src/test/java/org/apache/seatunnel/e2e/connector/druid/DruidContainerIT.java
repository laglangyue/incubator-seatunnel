package org.apache.seatunnel.e2e.connector.druid;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import java.util.stream.Stream;

public class DruidContainerIT extends TestSuiteBase implements TestResource {

    private static final String DOCKER_IMAGE = "laglangyue/druid:0.24.2";
    private static final String DRIVER_CLASS = "";
    private static final String HOST = "e2e_druid";
    private static final String DATABASE = "SYSDBA";
    private GenericContainer<?> dbServer;

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        dbServer = new GenericContainer<>(DOCKER_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases(HOST)
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)));
        dbServer.setPortBindings(Lists.newArrayList(
            // router
            String.format("%s:%s", 8888, 8888),
            // broker
            String.format("%s:%s", 8082, 8082)
        ));
        Startables.deepStart(Stream.of(dbServer)).join();
    }

    @Override
    @AfterAll
    public void tearDown() throws Exception {
        if (dbServer != null) {
            dbServer.close();
        }
    }

    @Test
    void testBrokerSchema() {

    }
}
