/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client.utils;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.collection.Seq;

import static org.apache.hudi.client.utils.ScalaConvertions.toSeq;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.lit;

/**
 * Temporary utils to facilitate DataFrame writer.
 * <p>
 * To be mismantled after code refactoring.
 */
public class SparkDataFrameWriterUtils {

  public static final Column[] JOIN_COLS = Stream.of(PARTITION_PATH_METADATA_FIELD, RECORD_KEY_METADATA_FIELD)
      .map(Column::new).toArray(Column[]::new);
  public static final Seq<String> JOIN_COL_NAMES_SEQ = toSeq(Arrays.stream(JOIN_COLS).map(Column::toString));
  public static final Column[] HOODIE_META_COLS = HOODIE_META_COLUMNS.stream().map(Column::new)
      .toArray(Column[]::new);
  public static final String[] HOODIE_META_COL_NAMES = HOODIE_META_COLUMNS.toArray(new String[0]);

  public static Dataset<Row> addHoodieKeyPartitionCols(Dataset<Row> df) {
    return df.withColumn(RECORD_KEY_METADATA_FIELD, callUDF("getKey", lit("uuid"), df.col("uuid")))
        .withColumn(PARTITION_PATH_METADATA_FIELD, callUDF("getPartition", df.col("ts")));
  }

  public static Map<String, String> getHoodieOptions(HoodieWriteConfig hoodieConfig) {
    Map<String, String> opts = new HashMap<>(hoodieConfig.getProps().entrySet().stream()
        .collect(Collectors.toMap(e -> String.valueOf(e.getKey()), e -> String.valueOf(e.getValue()))));
    opts.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "uuid");
    opts.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "year,month,day");
    opts.put("hoodie.datasource.write.table.type", "COPY_ON_WRITE");
    opts.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), "org.apache.hudi.keygen.ComplexKeyGenerator");
    opts.put(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "ts");
    opts.put("hoodie.datasource.write.operation", WriteOperationType.BULK_INSERT.value());
    opts.put(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key(), "true");
    opts.put(HoodieWriteConfig.BULK_INSERT_SORT_MODE.key(), "PARTITION_SORT");
    opts.put(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key(), "snappy");
    opts.put(HoodieMetadataConfig.ENABLE.key(), "true");
    return opts;
  }

}
