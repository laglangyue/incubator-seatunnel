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

package org.apache.seatunnel.translation.spark.source;

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.translation.spark.source.reader.batch.BatchSourceReader;
import org.apache.seatunnel.translation.spark.source.reader.micro.MicroBatchSourceReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.types.StructType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * spark 2.4 接口实现
 * DataSourceV2 v2数据源空接口，可以移除，ReadSupport 继承该接口
 * ReadSupport 支持读
 * MicroBatchReadSupport streamingContext的读
 * DataSourceRegister 数据源注册
 */
public class SeaTunnelSourceSupport
        implements DataSourceV2, ReadSupport, MicroBatchReadSupport, DataSourceRegister {
    private static final Logger LOG = LoggerFactory.getLogger(SeaTunnelSourceSupport.class);
    public static final String SEA_TUNNEL_SOURCE_NAME = "SeaTunnelSource";
    public static final Integer CHECKPOINT_INTERVAL_DEFAULT = 10000;

    @Override
    public String shortName() {
        // 把这个数据源 注册到spark 中
        return SEA_TUNNEL_SOURCE_NAME;
    }

    @Override
    public DataSourceReader createReader(StructType rowType, DataSourceOptions options) {
        // 重载接口, 用于可以通过rowType去指定schema
        return createReader(options);
    }

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        // 从Options 获取读取SeaTunnelSource 具体的实现类(真正读取数据的对象)
        SeaTunnelSource<SeaTunnelRow, ?, ?> seaTunnelSource = getSeaTunnelSource(options);
        int parallelism = options.getInt(CommonOptions.PARALLELISM.key(), 1);
        // 用spark的 批reader包装 SeaTunnelSource,以便spark能够识别SeaTunnelSource
        return new BatchSourceReader(seaTunnelSource, parallelism);
    }

    @Override
    public MicroBatchReader createMicroBatchReader(
            Optional<StructType> rowTypeOptional,
            String checkpointLocation,
            DataSourceOptions options) {
        // 创建微批 读取器，spark streaming，即将被删除
        SeaTunnelSource<SeaTunnelRow, ?, ?> seaTunnelSource = getSeaTunnelSource(options);
        Integer parallelism = options.getInt(CommonOptions.PARALLELISM.key(), 1);
        Integer checkpointInterval =
                options.getInt(
                        EnvCommonOptions.CHECKPOINT_INTERVAL.key(), CHECKPOINT_INTERVAL_DEFAULT);
        String checkpointPath =
                StringUtils.replacePattern(checkpointLocation, "sources/\\d+", "sources-state");
        Configuration configuration =
                SparkSession.getActiveSession().get().sparkContext().hadoopConfiguration();
        String hdfsRoot =
                options.get(Constants.HDFS_ROOT)
                        .orElse(FileSystem.getDefaultUri(configuration).toString());
        String hdfsUser = options.get(Constants.HDFS_USER).orElse("");
        Integer checkpointId = options.getInt(Constants.CHECKPOINT_ID, 1);
        return new MicroBatchSourceReader(
                seaTunnelSource,
                parallelism,
                checkpointId,
                checkpointInterval,
                checkpointPath,
                hdfsRoot,
                hdfsUser);
    }

    private SeaTunnelSource<SeaTunnelRow, ?, ?> getSeaTunnelSource(DataSourceOptions options) {
        return SerializationUtils.stringToObject(
                options.get(Constants.SOURCE_SERIALIZATION)
                        .orElseThrow(
                                () ->
                                        new UnsupportedOperationException(
                                                "Serialization information for the SeaTunnelSource is required")));
    }
}
