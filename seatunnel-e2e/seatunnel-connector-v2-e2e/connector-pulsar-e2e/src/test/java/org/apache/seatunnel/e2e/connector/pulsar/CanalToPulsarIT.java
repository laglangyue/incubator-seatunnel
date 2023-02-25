/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.pulsar;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.FLINK, EngineType.SPARK})
public class CanalToPulsarIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(CanalToPulsarIT.class);

    // ----------------------------------------------------------------------------
    // canal
    private static GenericContainer<?> CANAL_CONTAINER;


//    docker pull canal/canal-server:v1.1.2
//    docker run -d -it --link pulsar-mysql
//    -e canal.auto.scan=false
//    -e canal.destinations=test
//    -e canal.instance.master.address=pulsar-mysql:3306
//    -e canal.instance.dbUsername=root
//    -e canal.instance.dbPassword=canal
//    -e canal.instance.connectionCharset=UTF-8
//    -e canal.instance.tsdb.enable=true
//    -e canal.instance.gtidon=false
//    --name=pulsar-canal-server
//    -p 8000:8000 -p 2222:2222 -p 11111:11111 -p 11112:11112 -m 4096m canal/canal-server:v1.1.2

    private static final String CANAL_DOCKER_IMAGE = "chinayin/canal:1.1.6";

    private static final String CANAL_HOST = "canal_e2e";

    private static final int CANAL_PORT = 11111;

    private void createCanalContainer() {
        CANAL_CONTAINER =
                new GenericContainer<>(CANAL_DOCKER_IMAGE)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("canal/canal.properties"),
                                "/app/server/conf/canal.properties")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("canal/instance.properties"),
                                "/app/server/conf/example/instance.properties")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(CANAL_HOST)
                        .withCommand()
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(CANAL_DOCKER_IMAGE)));
        CANAL_CONTAINER.setPortBindings(
                com.google.common.collect.Lists.newArrayList(
                        String.format("%s:%s", CANAL_PORT, CANAL_PORT)));
    }

    // ----------------------------------------------------------------------------
    // mysql
    private static final String MYSQL_HOST = "mysql_e2e";

    private static final int MYSQL_PORT = 3306;

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "canal", "mysqluser", "mysqlpw");

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        MySqlContainer mySqlContainer =
                new MySqlContainer(version)
                        .withConfigurationOverride("docker/server-gtids/my.cnf")
                        .withSetupSQL("docker/setup.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withDatabaseName("canal")
                        .withUsername("st_user")
                        .withPassword("seatunnel")
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        mySqlContainer.setPortBindings(
                com.google.common.collect.Lists.newArrayList(
                        String.format("%s:%s", MYSQL_PORT, MYSQL_PORT)));
        return mySqlContainer;
    }

    // ----------------------------------------------------------------------------

    // docker run -it -p 6650:6650 -p 8080:8080 --mount source=pulsar/data,target=/pulsar/data
    // apachepulsar/pulsar:2.11.0 bin/pulsar standalone
    // pulsar
    private static final String PULSAR_IMAGE_NAME = "apachepulsar/pulsar:2.10.0";

    private static final String TOPIC = "test-canal-sink";

    private static final int PULSAR_BROKER_PORT = 6650;
    private static final int PULSAR_BROKER_REST_PORT = 8080;

    private static final String PULSAR_HOST = "kafkaCluster";

    private static PulsarContainer PULSAR_CONTAINER;

    private void createPulsarContainer() {
        PULSAR_CONTAINER =
                new PulsarContainer(DockerImageName.parse(PULSAR_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(PULSAR_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(PULSAR_IMAGE_NAME)));

        PULSAR_CONTAINER.setPortBindings(
                com.google.common.collect.Lists.newArrayList(
                        String.format("%s:%s", PULSAR_BROKER_PORT, PULSAR_BROKER_PORT),
                        String.format("%s:%s", PULSAR_BROKER_REST_PORT, PULSAR_BROKER_REST_PORT)));
    }

    @BeforeAll
    @Override
    public void startUp() throws ClassNotFoundException {

        LOG.info("The first stage: Starting Kafka containers...");
        createPulsarContainer();
        Startables.deepStart(Stream.of(PULSAR_CONTAINER)).join();
        LOG.info("Kafka Containers are started");

        LOG.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Mysql Containers are started");

        LOG.info("The third stage: Starting Canal containers...");
        createCanalContainer();
        Startables.deepStart(Stream.of(CANAL_CONTAINER)).join();
        LOG.info("Canal Containers are started");

        LOG.info("The fourth stage: Starting PostgreSQL container...");
        LOG.info("postgresql Containers are started");

        given().ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initKafkaConsumer);
        inventoryDatabase.createAndInitialize();
    }

    private void initKafkaConsumer() {}

    @Override
    public void tearDown() {
        MYSQL_CONTAINER.close();
        PULSAR_CONTAINER.close();
        CANAL_CONTAINER.close();
    }
}
