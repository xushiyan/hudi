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

package org.apache.hudi.hive.functional;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.testutils.NetworkTestUtils;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.hive.HoodieHiveClient;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.hive.NonPartitionedExtractor;
import org.apache.hudi.hive.testutils.HiveSyncFunctionalTestHarness;
import org.apache.hudi.hive.testutils.HiveTestUtil;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient.PartitionEvent;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient.PartitionEvent.PartitionEventType;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHiveSyncTool extends HiveSyncFunctionalTestHarness {

  private static final String TEST_DB_NAME = "testhivesynctool_testdb";

  private static Stream<Boolean> useJdbc() {
    return Stream.of(false, true);
  }

  private static Stream<Arguments> useJdbcAndSchemaFromCommitMetadata() {
    return Stream.of(
        Arguments.of(true, true),
        Arguments.of(true, false),
        Arguments.of(false, true),
        Arguments.of(false, false)
    );
  }

  HiveSyncConfig hiveSyncConfig;
  HiveTestUtil tableHelper;

  @BeforeEach
  public void setUp() throws IOException {
    createDatabases(TEST_DB_NAME);
    hiveSyncConfig = hiveSyncConf();
    hiveSyncConfig.databaseName = TEST_DB_NAME;
    tableHelper = new HiveTestUtil(hiveSyncConfig, fs(), hadoopConf());
  }

  @ParameterizedTest
  @MethodSource({"useJdbcAndSchemaFromCommitMetadata"})
  public void testBasicSync(boolean useJdbc, boolean useSchemaFromCommitMetadata) throws Exception {
    final String tableName = "basicsync_testtable";
    hiveSyncConfig.tableName = tableName;
    hiveSyncConfig.useJdbc = useJdbc;
    String instantTime = "100";
    tableHelper.createCOWTable1(instantTime, 5, useSchemaFromCommitMetadata);
    HoodieHiveClient hiveClient = hiveClient(hiveSyncConfig);
    assertFalse(hiveClient.doesTableExist(tableName),
        "Table " + tableName + " should not exist initially");
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, hiveConf(), fs());
    tool.syncHoodieTable();
    // we need renew the hiveclient after tool.syncHoodieTable(), because it will close hive
    // session, then lead to connection retry, we can see there is a exception at log.
    assertTrue(hiveClient.doesTableExist(tableName),
        "Table " + tableName + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(tableName).size(),
        hiveClient.getDataSchema().getColumns().size() + 1,
        "Hive Schema should match the table schema + partition field");
    assertEquals(5, hiveClient.scanTablePartitions(tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(tableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Adding of new partitions
    List<String> newPartition = Collections.singletonList("2050/01/01");
    hiveClient.addPartitionsToTable(tableName, Collections.emptyList());
    assertEquals(5, hiveClient.scanTablePartitions(tableName).size(),
        "No new partition should be added");
    hiveClient.addPartitionsToTable(tableName, newPartition);
    assertEquals(6, hiveClient.scanTablePartitions(tableName).size(),
        "New partition should be added");

    // Update partitions
    hiveClient.updatePartitionsToTable(tableName, Collections.emptyList());
    assertEquals(6, hiveClient.scanTablePartitions(tableName).size(),
        "Partition count should remain the same");
    hiveClient.updatePartitionsToTable(tableName, newPartition);
    assertEquals(6, hiveClient.scanTablePartitions(tableName).size(),
        "Partition count should remain the same");

    // Alter partitions
    // Manually change a hive partition location to check if the sync will detect
    // it and generate a partition update event for it.
    hiveClient.updateHiveSQL("ALTER TABLE `" + tableName
        + "` PARTITION (`datestr`='2050-01-01') SET LOCATION '/some/new/location'");

    List<Partition> hivePartitions = hiveClient.scanTablePartitions(tableName);
    List<String> writtenPartitionsSince = hiveClient.getPartitionsWrittenToSince(Option.empty());
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince);
    assertEquals(1, partitionEvents.size(), "There should be only one partition event");
    assertEquals(PartitionEventType.UPDATE, partitionEvents.iterator().next().eventType,
        "The one partition event must of type UPDATE");

    tool = new HiveSyncTool(hiveSyncConfig, hiveConf(), fs());
    tool.syncHoodieTable();
    // Sync should update the changed partition to correct path
    List<Partition> tablePartitions = hiveClient.scanTablePartitions(tableName);
    assertEquals(6, tablePartitions.size(), "The one partition we wrote should be added to hive");
    assertEquals(instantTime, hiveClient.getLastCommitTimeSynced(tableName).get(),
        "The last commit that was synced should be 100");
  }

  @ParameterizedTest
  @MethodSource({"useJdbcAndSchemaFromCommitMetadata"})
  public void testSyncWithProperties(boolean useJdbc, boolean useSchemaFromCommitMetadata) throws Exception {
    final String tableName = "syncprops_testtable";
    hiveSyncConfig.tableName = tableName;
    hiveSyncConfig.useJdbc = useJdbc;
    hiveSyncConfig.serdeProperties = "path=" + hiveSyncConfig.basePath;
    hiveSyncConfig.tableProperties = "tp_0=p0\ntp_1=p1";
    tableHelper.createCOWTable1("100", 5, useSchemaFromCommitMetadata);

    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, hiveConf(), fs());
    tool.syncHoodieTable();

    SessionState ss = SessionState.start(hiveConf());
    Driver hiveDriver = new org.apache.hadoop.hive.ql.Driver(hiveConf());
    String dbTableName = hiveSyncConfig.databaseName + "." + tableName;
    hiveDriver.run("SHOW TBLPROPERTIES " + dbTableName);
    List<String> tablePropsResults = new ArrayList<>();
    hiveDriver.getResults(tablePropsResults);

    String tblPropertiesWithoutDdlTime = String.join("\n",
        tablePropsResults.subList(0, tablePropsResults.size() - 1));
    assertEquals(
        "EXTERNAL\tTRUE\n"
        + "last_commit_time_sync\t100\n"
        + "tp_0\tp0\n"
        + "tp_1\tp1", tblPropertiesWithoutDdlTime);
    assertTrue(tablePropsResults.get(tablePropsResults.size() - 1).startsWith("transient_lastDdlTime"));

    List<String> serdePropsResults = new ArrayList<>();
    hiveDriver.run("SHOW CREATE TABLE " + dbTableName);
    hiveDriver.getResults(serdePropsResults);
    String ddl = String.join("\n", serdePropsResults);
    assertTrue(ddl.contains("'path'='" + hiveSyncConfig.basePath + "'"));
    hiveDriver.close();
    SessionState.endStart(ss);
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testSyncIncremental(boolean useJdbc) throws Exception {
    final String commitTime1 = "100";
    final String tableName = "syncincremental_testtable";
    hiveSyncConfig.tableName = tableName;
    hiveSyncConfig.useJdbc = useJdbc;
    tableHelper.createCOWTable1(commitTime1, 5, true);
    HoodieHiveClient hiveClient = hiveClient(hiveSyncConfig);
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, hiveConf(), fs());
    tool.syncHoodieTable();
    assertEquals(5, hiveClient.scanTablePartitions(tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime1, hiveClient.getLastCommitTimeSynced(tableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "101";
    tableHelper.addCOWPartitions1(1, true, true, dateTime, commitTime2);

    // Lets do the sync
    hiveClient = hiveClient(hiveSyncConfig);
    List<String> writtenPartitionsSince = hiveClient.getPartitionsWrittenToSince(Option.of(commitTime1));
    assertEquals(1, writtenPartitionsSince.size(), "We should have one partition written after 100 commit");
    List<Partition> hivePartitions = hiveClient.scanTablePartitions(tableName);
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince);
    assertEquals(1, partitionEvents.size(), "There should be only one partition event");
    assertEquals(PartitionEventType.ADD, partitionEvents.iterator().next().eventType, "The one partition event must of type ADD");

    tool = new HiveSyncTool(hiveSyncConfig, hiveConf(), fs());
    tool.syncHoodieTable();
    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(tableName).size(),
        "The one partition we wrote should be added to hive");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(tableName).get(),
        "The last commit that was synced should be 101");
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testSyncIncrementalWithSchemaEvolution(boolean useJdbc) throws Exception {
    final String commitTime1 = "100";
    final String tableName = "syncincremental_schemaevol_testtable";
    hiveSyncConfig.tableName = tableName;
    hiveSyncConfig.useJdbc = useJdbc;
    tableHelper.createCOWTable1(commitTime1, 5, true);
    HoodieHiveClient hiveClient = hiveClient(hiveSyncConfig);
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, hiveConf(), fs());
    tool.syncHoodieTable();

    int fields = hiveClient.getTableSchema(tableName).size();

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    final String commitTime2 = "101";
    tableHelper.addCOWPartitions1(1, false, true, dateTime, commitTime2);

    // Lets do the sync
    tool = new HiveSyncTool(hiveSyncConfig, hiveConf(), fs());
    tool.syncHoodieTable();

    assertEquals(fields + 3, hiveClient.getTableSchema(tableName).size(),
        "Hive Schema has evolved and should not be 3 more field");
    assertEquals("BIGINT", hiveClient.getTableSchema(tableName).get("favorite_number"),
        "Hive Schema has evolved - Field favorite_number has evolved from int to long");
    assertTrue(hiveClient.getTableSchema(tableName).containsKey("favorite_movie"),
        "Hive Schema has evolved - Field favorite_movie was added");

    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(tableName).size(),
        "The one partition we wrote should be added to hive");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(tableName).get(),
        "The last commit that was synced should be 101");
  }

  @ParameterizedTest
  @MethodSource("useJdbcAndSchemaFromCommitMetadata")
  public void testSyncMergeOnRead(boolean useJdbc, boolean useSchemaFromCommitMetadata) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    String instantTime = "100";
    String deltaCommitTime = "101";
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true,
        useSchemaFromCommitMetadata);

    String roTableName = HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_READ_OPTIMIZED_TABLE;
    HoodieHiveClient hiveClient = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    assertFalse(hiveClient.doesTableExist(roTableName), "Table " + HiveTestUtil.hiveSyncConfig.tableName + " should not exist initially");
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    assertTrue(hiveClient.doesTableExist(roTableName), "Table " + roTableName + " should exist after sync completes");

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size(),
          "Hive Schema should match the table schema + partition field");
    }

    assertEquals(5, hiveClient.scanTablePartitions(roTableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(deltaCommitTime, hiveClient.getLastCommitTimeSynced(roTableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    HiveTestUtil.addCOWPartitions(1, true, useSchemaFromCommitMetadata, dateTime, commitTime2);
    HiveTestUtil.addMORPartitions(1, true, false,
        useSchemaFromCommitMetadata, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    hiveClient = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the evolved table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClient.getTableSchema(roTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size(),
          "Hive Schema should match the evolved table schema + partition field");
    }
    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(roTableName).size(),
        "The 2 partitions we wrote should be added to hive");
    assertEquals(deltaCommitTime2, hiveClient.getLastCommitTimeSynced(roTableName).get(),
        "The last commit that was synced should be 103");
  }

  @ParameterizedTest
  @MethodSource("useJdbcAndSchemaFromCommitMetadata")
  public void testSyncMergeOnReadRT(boolean useJdbc, boolean useSchemaFromCommitMetadata) throws Exception {
    HiveTestUtil.hiveSyncConfig.useJdbc = useJdbc;
    String instantTime = "100";
    String deltaCommitTime = "101";
    String snapshotTableName = HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    HiveTestUtil.createMORTable(instantTime, deltaCommitTime, 5, true, useSchemaFromCommitMetadata);
    HoodieHiveClient hiveClientRT =
        new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    assertFalse(hiveClientRT.doesTableExist(snapshotTableName),
        "Table " + HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
            + " should not exist initially");

    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();

    assertTrue(hiveClientRT.doesTableExist(snapshotTableName),
        "Table " + HiveTestUtil.hiveSyncConfig.tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE
            + " should exist after sync completes");

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getSimpleSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size(),
          "Hive Schema should match the table schema + partition field");
    }

    assertEquals(5, hiveClientRT.scanTablePartitions(snapshotTableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(deltaCommitTime, hiveClientRT.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    HiveTestUtil.addCOWPartitions(1, true, useSchemaFromCommitMetadata, dateTime, commitTime2);
    HiveTestUtil.addMORPartitions(1, true, false, useSchemaFromCommitMetadata, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    tool = new HiveSyncTool(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);
    tool.syncHoodieTable();
    hiveClientRT = new HoodieHiveClient(HiveTestUtil.hiveSyncConfig, HiveTestUtil.getHiveConf(), HiveTestUtil.fileSystem);

    if (useSchemaFromCommitMetadata) {
      assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size()
              + HoodieRecord.HOODIE_META_COLUMNS.size(),
          "Hive Schema should match the evolved table schema + partition field");
    } else {
      // The data generated and schema in the data file do not have metadata columns, so we need a separate check.
      assertEquals(hiveClientRT.getTableSchema(snapshotTableName).size(),
          SchemaTestUtil.getEvolvedSchema().getFields().size() + HiveTestUtil.hiveSyncConfig.partitionFields.size(),
          "Hive Schema should match the evolved table schema + partition field");
    }
    // Sync should add the one partition
    assertEquals(6, hiveClientRT.scanTablePartitions(snapshotTableName).size(),
        "The 2 partitions we wrote should be added to hive");
    assertEquals(deltaCommitTime2, hiveClientRT.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was synced should be 103");
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testMultiPartitionKeySync(boolean useJdbc) throws Exception {
    hiveSyncConfig.useJdbc = useJdbc;
    hiveSyncConfig.partitionValueExtractorClass = MultiPartKeysValueExtractor.class.getCanonicalName();
    hiveSyncConfig.partitionFields = Arrays.asList("year", "month", "day");

    final String tableName = hiveSyncConfig.tableName;
    final String commitTime1 = "100";
    tableHelper.createCOWTable1(commitTime1, 5, true);

    HoodieHiveClient hiveClient = hiveClient(hiveSyncConfig);
    assertFalse(hiveClient.doesTableExist(tableName),
        "Table " + tableName + " should not exist initially");
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, hiveConf(), fs());
    tool.syncHoodieTable();
    assertTrue(hiveClient.doesTableExist(tableName),
        "Table " + tableName + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(tableName).size(),
        hiveClient.getDataSchema().getColumns().size() + 3,
        "Hive Schema should match the table schema + partition fields");
    assertEquals(5, hiveClient.scanTablePartitions(tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime1, hiveClient.getLastCommitTimeSynced(tableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");

    // HoodieHiveClient had a bug where partition vals were sorted
    // and stored as keys in a map. The following tests this particular case.
    // Now lets create partition "2010/01/02" and followed by "2010/02/01".
    final String commitTime2 = "101";
    tableHelper.addCOWPartition1("2010/01/02", true, true, commitTime2);

    hiveClient = hiveClient(hiveSyncConfig);
    List<String> writtenPartitionsSince = hiveClient.getPartitionsWrittenToSince(Option.of(commitTime1));
    assertEquals(1, writtenPartitionsSince.size(), "We should have one partition written after 100 commit");
    List<Partition> hivePartitions = hiveClient.scanTablePartitions(tableName);
    List<PartitionEvent> partitionEvents = hiveClient.getPartitionEvents(hivePartitions, writtenPartitionsSince);
    assertEquals(1, partitionEvents.size(), "There should be only one partition event");
    assertEquals(PartitionEventType.ADD, partitionEvents.iterator().next().eventType, "The one partition event must of type ADD");

    tool = new HiveSyncTool(hiveSyncConfig, hiveConf(), fs());
    tool.syncHoodieTable();

    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime2, hiveClient.getLastCommitTimeSynced(tableName).get(),
        "The last commit that was synced should be 101");

    // create partition "2010/02/01" and ensure sync works
    final String commitTime3 = "102";
    tableHelper.addCOWPartition1("2010/02/01", true, true, commitTime3);
    hiveClient = hiveClient(hiveSyncConfig);

    tool = new HiveSyncTool(hiveSyncConfig, hiveConf(), fs());
    tool.syncHoodieTable();

    assertTrue(hiveClient.doesTableExist(tableName),
        "Table " + tableName + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(tableName).size(),
        hiveClient.getDataSchema().getColumns().size() + 3,
        "Hive Schema should match the table schema + partition fields");
    assertEquals(7, hiveClient.scanTablePartitions(tableName).size(),
        "Table partitions should match the number of partitions we wrote");
    assertEquals(commitTime3, hiveClient.getLastCommitTimeSynced(tableName).get(),
        "The last commit that was synced should be updated in the TBLPROPERTIES");
    assertEquals(1, hiveClient.getPartitionsWrittenToSince(Option.of(commitTime2)).size());
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testNonPartitionedSync(boolean useJdbc) throws Exception {
    hiveSyncConfig.useJdbc = useJdbc;
    final String tableName = hiveSyncConfig.tableName;
    tableHelper.createCOWTable1("100", 5, true);

    // Set partition value extractor to NonPartitionedExtractor
    hiveSyncConfig.partitionValueExtractorClass = NonPartitionedExtractor.class.getCanonicalName();
    hiveSyncConfig.partitionFields = Arrays.asList("year", "month", "day");

    HoodieHiveClient hiveClient = hiveClient(hiveSyncConfig);
    assertFalse(hiveClient.doesTableExist(tableName),
        "Table " + tableName + " should not exist initially");
    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, hiveConf(), fs());
    tool.syncHoodieTable();
    assertTrue(hiveClient.doesTableExist(tableName),
        "Table " + tableName + " should exist after sync completes");
    assertEquals(hiveClient.getTableSchema(tableName).size(),
        hiveClient.getDataSchema().getColumns().size(),
        "Hive Schema should match the table schema，ignoring the partition fields");
    assertEquals(0, hiveClient.scanTablePartitions(tableName).size(),
        "Table should not have partitions because of the NonPartitionedExtractor");
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testReadSchemaForMOR(boolean useJdbc) throws Exception {
    final String tableName = "readMORschema_testtable_jdbc_" + useJdbc;
    hiveSyncConfig.tableName = tableName;
    hiveSyncConfig.useJdbc = useJdbc;
    final String commitTime = "100";
    final String snapshotTableName = tableName + HiveSyncTool.SUFFIX_SNAPSHOT_TABLE;
    tableHelper.createMORTable1(commitTime, "", 5, false, true);
    logHiveTable(hiveSyncConfig.databaseName, tableName, snapshotTableName);
    HoodieHiveClient hiveClient = hiveClient(hiveSyncConfig, HoodieTableType.MERGE_ON_READ);

    assertFalse(hiveClient.doesTableExist(snapshotTableName), "Table " + snapshotTableName
        + " should not exist initially");

    // Lets do the sync
    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, hiveConf(), fs());
    tool.syncHoodieTable();

    assertTrue(hiveClient.doesTableExist(snapshotTableName), "Table " + snapshotTableName
        + " should exist after sync completes");

    // Schema being read from compacted base files
    assertEquals(hiveClient.getTableSchema(snapshotTableName).size(),
        SchemaTestUtil.getSimpleSchema().getFields().size() + hiveSyncConfig.partitionFields.size()
            + HoodieRecord.HOODIE_META_COLUMNS.size(),
        "Hive Schema should match the table schema + partition field");
    assertEquals(5, hiveClient.scanTablePartitions(snapshotTableName).size(), "Table partitions should match the number of partitions we wrote");

    // Now lets create more partitions and these are the only ones which needs to be synced
    ZonedDateTime dateTime = ZonedDateTime.now().plusDays(6);
    String commitTime2 = "102";
    String deltaCommitTime2 = "103";

    tableHelper.addMORPartitions1(1, true, false, true, dateTime, commitTime2, deltaCommitTime2);
    // Lets do the sync
    tool = new HiveSyncTool(hiveSyncConfig, hiveConf(), fs());
    tool.syncHoodieTable();
    hiveClient = hiveClient(hiveSyncConfig, HoodieTableType.MERGE_ON_READ);

    // Schema being read from the log files
    assertEquals(hiveClient.getTableSchema(snapshotTableName).size(),
        SchemaTestUtil.getEvolvedSchema().getFields().size() + hiveSyncConfig.partitionFields.size()
            + HoodieRecord.HOODIE_META_COLUMNS.size(),
        "Hive Schema should match the evolved table schema + partition field");
    // Sync should add the one partition
    assertEquals(6, hiveClient.scanTablePartitions(snapshotTableName).size(), "The 1 partition we wrote should be added to hive");
    assertEquals(deltaCommitTime2, hiveClient.getLastCommitTimeSynced(snapshotTableName).get(),
        "The last commit that was synced should be 103");
  }

  @Test
  public void testConnectExceptionIgnoreConfigSet() throws Exception {
    final String tableName = "ignoreexception_testtable";
    hiveSyncConfig.tableName = tableName;
    hiveSyncConfig.useJdbc = true;
    hiveSyncConfig.ignoreExceptions = true;
    metaClient(hiveSyncConfig, HoodieTableType.COPY_ON_WRITE);
    HoodieHiveClient hiveClient = hiveClient(hiveSyncConfig);
    assertFalse(hiveClient.doesTableExist(tableName),
        "Table " + tableName + " should not exist before hive sync");

    // change to an unused port then do hive sync
    hiveSyncConfig.jdbcUrl = hiveSyncConfig.jdbcUrl
        .replace(String.valueOf(hiveService().getHiveServerPort()), String.valueOf(NetworkTestUtils.nextFreePort()));
    HiveSyncTool tool = new HiveSyncTool(hiveSyncConfig, hiveConf(), fs());
    tool.syncHoodieTable();

    assertFalse(hiveClient.doesTableExist(tableName),
        "Table " + tableName + " should not exist after hive sync");
  }

  @ParameterizedTest
  @MethodSource("useJdbc")
  public void testTypeConverter(boolean useJdbc) throws Exception {
    final String tableName = "typeconverter_testtable";
    hiveSyncConfig.tableName = tableName;
    hiveSyncConfig.useJdbc = useJdbc;
    metaClient(hiveSyncConfig, HoodieTableType.COPY_ON_WRITE);
    HoodieHiveClient hiveClient = hiveClient(hiveSyncConfig);
    String tableAbsoluteName = String.format(" `%s.%s` ", hiveSyncConfig.databaseName, tableName);
    String dropTableSql = String.format("DROP TABLE IF EXISTS %s ", tableAbsoluteName);
    String createTableSqlPrefix = String.format("CREATE TABLE IF NOT EXISTS %s ", tableAbsoluteName);
    String errorMsg = "An error occurred in decimal type converting.";
    hiveClient.updateHiveSQL(dropTableSql);

    // test one column in DECIMAL
    String oneTargetColumnSql = createTableSqlPrefix + "(`decimal_col` DECIMAL(9,8), `bigint_col` BIGINT)";
    hiveClient.updateHiveSQL(oneTargetColumnSql);
    assertTrue(hiveClient.getTableSchema(tableName).containsValue("DECIMAL(9,8)"), errorMsg);
    hiveClient.updateHiveSQL(dropTableSql);

    // test multiple columns in DECIMAL
    String multipleTargetColumnSql =
        createTableSqlPrefix + "(`decimal_col1` DECIMAL(9,8), `bigint_col` BIGINT, `decimal_col2` DECIMAL(7,4))";
    hiveClient.updateHiveSQL(multipleTargetColumnSql);
    assertTrue(hiveClient.getTableSchema(tableName).containsValue("DECIMAL(9,8)")
        && hiveClient.getTableSchema(tableName).containsValue("DECIMAL(7,4)"), errorMsg);
    hiveClient.updateHiveSQL(dropTableSql);

    // test no columns in DECIMAL
    String noTargetColumnsSql = createTableSqlPrefix + "(`bigint_col` BIGINT)";
    hiveClient.updateHiveSQL(noTargetColumnsSql);
    assertTrue(hiveClient.getTableSchema(tableName).size() == 1 && hiveClient.getTableSchema(tableName)
        .containsValue("BIGINT"), errorMsg);
    hiveClient.updateHiveSQL(dropTableSql);
  }

}
