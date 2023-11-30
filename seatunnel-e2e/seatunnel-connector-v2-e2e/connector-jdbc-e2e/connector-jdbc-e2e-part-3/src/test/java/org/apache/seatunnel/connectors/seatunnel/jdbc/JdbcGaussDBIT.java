package org.apache.seatunnel.connectors.seatunnel.jdbc;

import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerLoggerFactory;

@Slf4j
public class JdbcGaussDBIT extends AbstractJdbcIT {

  private static final String GAUSSDB_IMAGE = "opengauss/opengauss:5.0.0";

  private static final String GAUSSDB_DRIVER_JAR =
      "https://repo1.maven.org/maven2/com/huaweicloud/dws/huaweicloud-dws-jdbc/8.3.0-200/huaweicloud-dws-jdbc-8.3.0-200.jar";


  private GenericContainer<?> container;

  private static final String CONTAINER_HOST = "gaussdb-e2e";

  private static final String DATABASE = "postgres";

  private static final String SOURCE_TABLE = "SOURCE";

  private static final String SINK_TABLE = "SINK";

  private static final String GAUSSDB_URL = "jdbc:gaussdb://" + HOST + ":%s/%s";

  private static final String DRIVER_CLASS = "com.ibm.db2.jcc.DB2Driver";

  private static final String PASSWORD = "openGauss@123";

  public static final String USER = "gaussdb";

  private static final int PORT = 5432;

  private static final int LOCAL_PORT = 54320;

  public static final List<String> CONFIG_FILE = Lists.newArrayList();


  private static final String CREATE_SOURCE =
      "CREATE TABLE IF NOT EXISTS" + SOURCE_TABLE + "(\n"
          + "  gid SERIAL PRIMARY KEY,\n"
          + "  text_col TEXT,\n"
          + "  varchar_col VARCHAR(255),\n"
          + "  char_col CHAR(10),\n"
          + "  boolean_col bool,\n"
          + "  smallint_col int2,\n"
          + "  integer_col int4,\n"
          + "  bigint_col BIGINT,\n"
          + "  decimal_col DECIMAL(10, 2),\n"
          + "  numeric_col NUMERIC(8, 4),\n"
          + "  real_col float4,\n"
          + "  double_precision_col float8,\n"
          + "  smallserial_col SMALLSERIAL,\n"
          + "  serial_col SERIAL,\n"
          + "  bigserial_col BIGSERIAL,\n"
          + "  date_col DATE,\n"
          + "  timestamp_col TIMESTAMP,\n"
          + "  bpchar_col BPCHAR(10),\n"
          + "  age INT NOT null,\n"
          + "  name VARCHAR(255) NOT null,\n"
          + "  point geometry(POINT, 4326),\n"
          + "  linestring geometry(LINESTRING, 4326),\n"
          + "  polygon_colums geometry(POLYGON, 4326),\n"
          + "  multipoint geometry(MULTIPOINT, 4326),\n"
          + "  multilinestring geometry(MULTILINESTRING, 4326),\n"
          + "  multipolygon geometry(MULTIPOLYGON, 4326),\n"
          + "  geometrycollection geometry(GEOMETRYCOLLECTION, 4326),\n"
          + "  geog geography(POINT, 4326),\n"
          + "  json_col json NOT NULL,\n"
          + "  jsonb_col jsonb NOT NULL,\n"
          + "  xml_col xml NOT NULL\n"
          + ")";

  private static final String CREATE_SINK =
      "CREATE TABLE IF NOT EXISTS " + SINK_TABLE + " (\n"
          + "    gid SERIAL PRIMARY KEY,\n"
          + "    text_col TEXT,\n"
          + "    varchar_col VARCHAR(255),\n"
          + "    char_col CHAR(10),\n"
          + "    boolean_col bool,\n"
          + "    smallint_col int2,\n"
          + "    integer_col int4,\n"
          + "    bigint_col BIGINT,\n"
          + "    decimal_col DECIMAL(10, 2),\n"
          + "    numeric_col NUMERIC(8, 4),\n"
          + "    real_col float4,\n"
          + "    double_precision_col float8,\n"
          + "    smallserial_col SMALLSERIAL,\n"
          + "    serial_col SERIAL,\n"
          + "    bigserial_col BIGSERIAL,\n"
          + "    date_col DATE,\n"
          + "    timestamp_col TIMESTAMP,\n"
          + "    bpchar_col BPCHAR(10),\n"
          + "    age int4 NOT NULL,\n"
          + "    name varchar(255) NOT NULL,\n"
          + "    point varchar(2000) NULL,\n"
          + "    linestring varchar(2000) NULL,\n"
          + "    polygon_colums varchar(2000) NULL,\n"
          + "    multipoint varchar(2000) NULL,\n"
          + "    multilinestring varchar(2000) NULL,\n"
          + "    multipolygon varchar(2000) NULL,\n"
          + "    geometrycollection varchar(2000) NULL,\n"
          + "    geog varchar(2000) NULL,\n"
          + "    json_col json NOT NULL \n,"
          + "    jsonb_col jsonb NOT NULL,\n"
          + "    xml_col xml NOT NULL\n"
          + "  )";

  @TestContainerExtension
  private final ContainerExtendedFactory extendedFactory =
      container -> {
        Container.ExecResult extraCommands =
            container.execInContainer(
                "bash",
                "-c",
                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && curl -O "
                    + GAUSSDB_DRIVER_JAR);
        Assertions.assertEquals(0, extraCommands.getExitCode());
      };


  @AfterAll
  @Override
  public void tearDown() {
    if (container != null) {
      container.stop();
    }
  }


  @Override
  JdbcCase getJdbcCase() {
    Map<String, String> containerEnv = new HashMap<>();
    String jdbcUrl = String.format(GAUSSDB_URL, CONTAINER_HOST, LOCAL_PORT);
    Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
    String[] fieldNames = testDataSet.getKey();
    String insertSql = insertTable(DATABASE, SOURCE_TABLE, fieldNames);
    return JdbcCase.builder()
        .dockerImage(GAUSSDB_IMAGE)
        .networkAliases(CONTAINER_HOST)
        .containerEnv(containerEnv)
        .driverClass(DRIVER_CLASS)
        .host(HOST)
        .port(PORT)
        .localPort(LOCAL_PORT)
        .jdbcTemplate(GAUSSDB_URL)
        .jdbcUrl(jdbcUrl)
        .userName(USER)
        .password(PASSWORD)
        .database(DATABASE)
        .sourceTable(SOURCE_TABLE)
        .sinkTable(SINK_TABLE)
        .createSql(CREATE_SOURCE)
        .configFile(CONFIG_FILE)
        .insertSql(insertSql)
        .testData(testDataSet)
        .build();
  }

  @Override
  void compareResult(String executeKey) {
  }

  @Override
  String driverUrl() {
    return GAUSSDB_URL;
  }

  @Override
  GenericContainer<?> initContainer() {
    // docker run --name opengauss --privileged=true -d -e GS_PASSWORD=openGauss@123 -p 15434:5432 opengauss/opengauss:5.0.0
    container = new GenericContainer<>()
        .withNetwork(TestSuiteBase.NETWORK)
        .withNetworkAliases("gaussdb")
        .withPrivilegedMode(true)
        .withEnv("GS_PASSWORD", PASSWORD)
        .withExposedPorts(5432)
        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(GAUSSDB_IMAGE)));
    container.setPortBindings(Lists.newArrayList(String.format("%s:%s", LOCAL_PORT, PORT)));
    return container;
  }

  @Override
  public String quoteIdentifier(String field) {
    return "\"" + field + "\"";
  }


  @Override
  Pair<String[], List<SeaTunnelRow>> initTestData() {
    String[] fieldNames =
        new String[]{
            "boolean_col",
            "smallint_col",
            "integer_col",
            "bigint_col",
            "gid",
            "decimal_col",
            "numeric_col",
            "real_col",
            "double_precision_col",
            "smallserial_col",
            "serial_col",
            "bigserial_col",

            "text_col",
            "varchar_col",
            "char_col",
            "date_col",
            "timestamp_col",

            "bpchar_col",
            "age",
            "name",
            "point",
            "linestring",
            "polygon_colums",
            "multipoint",
            "multilinestring",
            "multipolygon",
            "geometrycollection",
            "geog",
            "json_col",
            "jsonb_col",
            "xml_cl"
        };

    List<SeaTunnelRow> rows = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      SeaTunnelRow row =
          new SeaTunnelRow(
              new Object[]{
                  i % 2 == 0 ? Boolean.TRUE : Boolean.FALSE,
                  Short.valueOf("1"),
                  i,
                  Long.parseLong("1"),
                  Long.parseLong("1"),
                  BigDecimal.valueOf(i, 0),
                  BigDecimal.valueOf(i, 18),
                  Float.parseFloat("1.1"),
                  Double.parseDouble("1.1"),
                  Long.parseLong("1"),
                  Long.parseLong("1"),
                  Long.parseLong("1"),
                  String.format("f1_%s", i),
                  "f",
                  'f',
                  Date.valueOf(LocalDate.now()),
                  Timestamp.valueOf(LocalDateTime.now()),
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null,
                  null
              });
      rows.add(row);
    }
    return Pair.of(fieldNames, rows);
  }

}
