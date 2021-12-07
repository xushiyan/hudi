/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.functional;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.HiveSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;

import static org.apache.hudi.common.testutils.Assertions.assertStreamEquals;
import static org.apache.hudi.common.util.CollectionUtils.createImmutableMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Basic tests against {@link HiveSchemaProvider}.
 */
@Tag("functional")
public class TestHiveSchemaProvider extends SparkClientFunctionalTestHarness {
  private static final String DB_NAME = "schema_registry";
  private static final String SOURCE_TABLE_NAME = "source_schema_tab";
  private static final String TARGET_TABLE_NAME = "target_schema_tab";
  private static final String CREATE_TABLE_SQL_TEMPLATE = "CREATE TABLE IF NOT EXISTS `%s`.`%s`(\n"
      + "`id` BIGINT,\n"
      + "`name` STRING,\n"
      + "`num1` INT,\n"
      + "`num2` BIGINT,\n"
      + "`num3` DECIMAL(20,0),\n"
      + "`num4` TINYINT,\n"
      + "`num5` FLOAT,\n"
      + "`num6` DOUBLE,\n"
      + "`bool` BOOLEAN,\n"
      + "`bin` BINARY\n"
      + ")";

  private TypedProperties props;

  public SparkConf conf() {
    return conf(Collections.singletonMap("spark.sql.catalogImplementation", "hive"));
  }

  @BeforeEach
  public void initTables() {
    createSchemaTable(DB_NAME, SOURCE_TABLE_NAME);
    createSchemaTable(DB_NAME, TARGET_TABLE_NAME);
    props = new TypedProperties();
    props.putAll(createImmutableMap(
        Pair.of("hoodie.deltastreamer.schemaprovider.source.schema.hive.database", DB_NAME),
        Pair.of("hoodie.deltastreamer.schemaprovider.source.schema.hive.table", SOURCE_TABLE_NAME),
        Pair.of("hoodie.deltastreamer.schemaprovider.target.schema.hive.database", DB_NAME),
        Pair.of("hoodie.deltastreamer.schemaprovider.target.schema.hive.table", TARGET_TABLE_NAME)
    ));
  }

  @AfterEach
  public void dropTables() {
    dropSchemaTable(DB_NAME, SOURCE_TABLE_NAME);
    dropSchemaTable(DB_NAME, TARGET_TABLE_NAME);
  }

  @Test
  public void testSourceTargetSchemaEqualToOriginals() throws Exception {
    SchemaProvider schemaProvider = UtilHelpers.createSchemaProvider(HiveSchemaProvider.class.getName(), props, jsc());
    Schema sourceSchema = schemaProvider.getSourceSchema();
    Schema targetSchema = schemaProvider.getTargetSchema();
    Schema originalSourceSchema = new Schema.Parser().parse(
        UtilitiesTestBase.Helpers.readFile("delta-streamer-config/hive_schema_provider_source.avsc")
    );
    Schema originalTargetSchema = new Schema.Parser().parse(
        UtilitiesTestBase.Helpers.readFile("delta-streamer-config/hive_schema_provider_target.avsc"));
    assertStreamEquals(
        originalSourceSchema.getFields().stream().map(Schema.Field::name),
        sourceSchema.getFields().stream().map(Schema.Field::name),
        "schema fields should be matched."
    );
    assertStreamEquals(
        originalTargetSchema.getFields().stream().map(Schema.Field::name),
        targetSchema.getFields().stream().map(Schema.Field::name),
        "schema fields should be matched."
    );
  }

  @Test
  public void testMisconfiguredTableShouldThrow() {
    props.put("hoodie.deltastreamer.schemaprovider.source.schema.hive.table", "non_exist_source_table");
    Throwable t = assertThrows(IOException.class, () -> UtilHelpers.createSchemaProvider(HiveSchemaProvider.class.getName(), props, jsc()));
    Throwable rootCause = ((InvocationTargetException) t.getCause().getCause()).getTargetException().getCause();
    assertEquals(NoSuchTableException.class, rootCause.getClass());
  }

  private void createSchemaTable(String dbName, String tableName) {
    spark().sql(String.format("CREATE DATABASE IF NOT EXISTS %s", dbName));
    spark().sql(String.format(CREATE_TABLE_SQL_TEMPLATE, dbName, tableName));
  }

  private void dropSchemaTable(String dbName, String tableName) {
    spark().sql(String.format("DROP TABLE `%s`.`%s`", dbName, tableName));
  }
}
