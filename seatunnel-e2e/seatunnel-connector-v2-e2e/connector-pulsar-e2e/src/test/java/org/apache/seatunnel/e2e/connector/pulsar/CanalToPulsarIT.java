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
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.google.common.collect.Lists;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * canal server producer data to pulsar, st-cdc is consumer reference:
 * https://pulsar.apache.org/docs/2.11.x/io-canal-source/
 */
@DisabledOnContainer(
        value = {},
        type = {EngineType.FLINK, EngineType.SPARK})
public class CanalToPulsarIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(CanalToPulsarIT.class);

    // ----------------------------------------------------------------------------
    // mysql
    private static final String MYSQL_HOST = "mysql.e2e";

    private static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER = "st_user";
    public static final String MYSQL_PASSWORD = "seatunnel";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V5_7);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "canal", "mysqluser", "mysqlpw");

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        MySqlContainer mySqlContainer =
                new MySqlContainer(version)
                        .withConfigurationOverride("mysql/server-gtids/my.cnf")
                        .withSetupSQL("mysql/setup.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withDatabaseName("canal")
                        .withUsername(MYSQL_USER)
                        .withPassword(MYSQL_PASSWORD)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));
        mySqlContainer.setPortBindings(
                com.google.common.collect.Lists.newArrayList(
                        String.format("%s:%s", MYSQL_PORT, MYSQL_PORT)));
        return mySqlContainer;
    }

    // ----------------------------------------------------------------------------
    // canal
    private static GenericContainer<?> CANAL_CONTAINER;

    private static final String CANAL_DOCKER_IMAGE = "canal/canal-server:v1.1.2";

    private static final String CANAL_HOST = "canal.e2e";

    private void createCanalContainer() {
        CANAL_CONTAINER =
                new GenericContainer<>(CANAL_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(CANAL_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(CANAL_DOCKER_IMAGE)));
        CANAL_CONTAINER.setPortBindings(
                Lists.newArrayList("8000:8000", "2222:2222", "11111:11111", "11112:11112"));

        CANAL_CONTAINER
                .withEnv("canal.auto.scan", "false")
                .withEnv("canal.destinations", "test")
                .withEnv(
                        "canal.instance.master.address",
                        String.format("%s:%s", MYSQL_HOST, MYSQL_PORT))
                .withEnv("canal.instance.dbUsername", MYSQL_USER)
                .withEnv("canal.instance.dbPassword", MYSQL_PASSWORD)
                .withEnv("canal.instance.connectionCharset", "UTF-8")
                .withEnv("canal.instance.tsdb.enable", "true")
                .withEnv("canal.instance.gtidon", "false");
    }

    // ----------------------------------------------------------------------------
    // pulsar container
    // download canal connector is so slowly,make it with canal connector from apache/pulsar
    private static final String PULSAR_IMAGE_NAME = "laglangyue/pulsar_canal:2.3";

    private static final int PULSAR_BROKER_PORT = 6650;
    private static final int PULSAR_BROKER_REST_PORT = 8080;

    private static final String PULSAR_HOST = "pulsar.e2e";

    private static GenericContainer<?> PULSAR_CONTAINER;

    private void createPulsarContainer() {
        PULSAR_CONTAINER =
                new GenericContainer<>(DockerImageName.parse(PULSAR_IMAGE_NAME))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(PULSAR_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(PULSAR_IMAGE_NAME)));

        PULSAR_CONTAINER.setPortBindings(
                com.google.common.collect.Lists.newArrayList(
                        String.format("%s:%s", PULSAR_BROKER_PORT, PULSAR_BROKER_PORT),
                        String.format("%s:%s", PULSAR_BROKER_REST_PORT, PULSAR_BROKER_REST_PORT)));

        // canal connectors config
        PULSAR_CONTAINER.withCopyFileToContainer(
                MountableFile.forClasspathResource("pulsar/canal-mysql-source-config.yaml"),
                "/pulsar/conf/");
        // start connectors cmd
        PULSAR_CONTAINER.withCopyFileToContainer(
                MountableFile.forClasspathResource("pulsar/start_canal_connector.sh"), "/pulsar/");
    }

    private void startPulsarCanalConnector()
            throws IOException, InterruptedException, PulsarAdminException {
        Container.ExecResult chmod =
                PULSAR_CONTAINER.execInContainer(
                        "chmod", "777", "/pulsar/start_canal_connector.sh");
        Assertions.assertEquals(chmod.getExitCode(), 0);
        // exec docker command backend
        Container.ExecResult execResult =
                PULSAR_CONTAINER.execInContainer(
                        "/bin/sh",
                        "-c",
                        "nohup /pulsar/start_canal_connector.sh > /pulsar/canal.log 2>&1 &");
        // ensure connector start
        PulsarAdmin pulsarAdmin =
                PulsarAdmin.builder()
                        .serviceHttpUrl(
                                String.format(
                                        "http://%s:%s",
                                        PULSAR_CONTAINER.getHost(), PULSAR_BROKER_REST_PORT))
                        .build();
        while (true) {
            try {
                if (!pulsarAdmin.topics().getList("public/default").isEmpty()) {
                    break;
                }
            } catch (Exception ignore) {
            }
            Thread.sleep(1000);
        }
        pulsarAdmin.close();
    }

    @BeforeAll
    @Override
    public void startUp() throws IOException, InterruptedException, PulsarAdminException {
        LOG.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Mysql Containers are started");

        LOG.info("The third stage: Starting Canal containers...");
        createCanalContainer();
        Startables.deepStart(Stream.of(CANAL_CONTAINER)).join();
        LOG.info("Canal Containers are started");

        LOG.info("The first stage: Starting Pulsar containers...");
        createPulsarContainer();
        Startables.deepStart(Stream.of(PULSAR_CONTAINER)).join();
        LOG.info("Pulsar Containers are started");
        LOG.info("start pulsar canal connector");
        startPulsarCanalConnector();
        LOG.info("pulsar canal connector start success");

        inventoryDatabase.createAndInitialize();
    }

    @Override
    public void tearDown() {
        MYSQL_CONTAINER.close();
        PULSAR_CONTAINER.close();
        CANAL_CONTAINER.close();
    }

    @TestTemplate
    void testCanalFormatMessages(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/cdc_canal_pulsar_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    void testCanalFormatMessagesToMysql(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        Container.ExecResult execResult = container.executeJob("/cdc_canal_pulsar_to_mysql.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        Set<List<Object>> actual = new HashSet<>();
        try (Connection connection =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("select * from products_assert");
                while (resultSet.next()) {
                    List<Object> row =
                            Arrays.asList(
                                    resultSet.getInt("id"),
                                    resultSet.getString("name"),
                                    resultSet.getString("description"),
                                    resultSet.getString("weight"));
                    actual.add(row);
                }
            }
        }
        Set<List<Object>> expected =
                Stream.<List<Object>>of(
                                Arrays.asList(102, "car battery", "12V car battery", "8.1"),
                                Arrays.asList(
                                        103,
                                        "12-pack drill bits",
                                        "12-pack of drill bits with sizes ranging from #40 to #3",
                                        "0.8"),
                                Arrays.asList(104, "hammer", "12oz carpenter's hammer", "0.75"),
                                Arrays.asList(105, "hammer", "14oz carpenter's hammer", "0.875"),
                                Arrays.asList(106, "hammer", "16oz carpenter's hammer", "1.0"),
                                Arrays.asList(
                                        108, "jacket", "water resistent black wind breaker", "0.1"),
                                Arrays.asList(101, "scooter", "Small 2-wheel scooter", "4.56"),
                                Arrays.asList(107, "rocks", "box of assorted rocks", "7.88"))
                        .collect(Collectors.toSet());
        Assertions.assertIterableEquals(expected, actual);
    }
}
