package org.apache.seatunnel.e2e.connector.druid;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import com.google.common.collect.Lists;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

public class DruidContainerIT extends TestSuiteBase implements TestResource {

    private static final String DOCKER_IMAGE = "laglangyue/druid:0.24.2";
    private static final String DRIVER_CLASS = "dm.jdbc.driver.DmDriver";
    private static final String HOST = "e2e_druid";
    private static final String DATABASE = "SYSDBA";
    private GenericContainer<?> dbServer;

    @Override
    public void startUp() throws Exception {
        dbServer = new GenericContainer<>(DOCKER_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases(HOST)
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)));
        dbServer.setPortBindings(Lists.newArrayList(
            String.format("%s:%s", 8888, 8888),
            String.format("%s:%s", 8082, 8082)
        ));
    }

    @Override
    public void tearDown() throws Exception {

    }
}
