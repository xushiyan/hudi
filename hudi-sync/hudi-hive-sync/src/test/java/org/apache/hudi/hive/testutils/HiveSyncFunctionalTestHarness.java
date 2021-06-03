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

package org.apache.hudi.hive.testutils;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.minicluster.ZookeeperTestService;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HiveSyncFunctionalTestHarness {

  private static transient Configuration hadoopConf;
  private static transient HiveTestService hiveTestService;
  private static transient ZookeeperTestService zookeeperTestService;

  /**
   * An indicator of the initialization status.
   */
  protected boolean initialized = false;
  @TempDir
  protected java.nio.file.Path tempDir;
  protected List<String> createdHiveTableFullNames = new ArrayList<>();
  protected List<HoodieHiveClient> createdHiveClients = new ArrayList<>();

  public String basePath() {
    return tempDir.toAbsolutePath().toString();
  }

  public Configuration hadoopConf() {
    return hadoopConf;
  }

  public FileSystem fs() throws IOException {
    return FileSystem.get(hadoopConf);
  }

  public HiveTestService hiveService() {
    return hiveTestService;
  }

  public HiveConf hiveConf() {
    return hiveTestService.getHiveServer().getHiveConf();
  }

  public ZookeeperTestService zkService() {
    return zookeeperTestService;
  }

  public HiveSyncConfig hiveSyncConf() throws IOException {
    HiveSyncConfig conf = new HiveSyncConfig();
    conf.jdbcUrl = hiveTestService.getJdbcHive2Url();
    conf.hiveUser = "";
    conf.hivePass = "";
    conf.databaseName = "hivesynctestdb";
    conf.tableName = "hivesynctesttable";
    conf.basePath = Files.createDirectories(tempDir.resolve("hivesynctestcase-" + Instant.now().toEpochMilli())).toString();
    conf.assumeDatePartitioning = true;
    conf.usePreApacheInputFormat = false;
    conf.partitionFields = Collections.singletonList("datestr");
    return conf;
  }

  public HoodieHiveClient hiveClient(HiveSyncConfig hiveSyncConfig) throws IOException {
    return hiveClient(hiveSyncConfig, HoodieTableType.COPY_ON_WRITE);
  }

  public HoodieHiveClient hiveClient(HiveSyncConfig hiveSyncConfig, HoodieTableType tableType) throws IOException {
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(tableType)
        .setTableName(hiveSyncConfig.tableName)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(hadoopConf, hiveSyncConfig.basePath);
    HoodieHiveClient hiveClient = new HoodieHiveClient(hiveSyncConfig, hiveConf(), fs());
    createdHiveClients.add(hiveClient);
    return hiveClient;
  }

  public HoodieTableMetaClient metaClient(HiveSyncConfig hiveSyncConfig, HoodieTableType tableType) throws IOException {
    return HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(tableType)
        .setTableName(hiveSyncConfig.tableName)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(hadoopConf(), hiveSyncConfig.basePath);
  }

  public void createDatabases(String... databases) throws IOException {
    HiveSyncConfig hiveSyncConfig = hiveSyncConf();
    HoodieHiveClient hiveClient = hiveClient(hiveSyncConfig);
    for (String database : databases) {
      hiveClient.updateHiveSQL("create database if not exists " + database);
    }
    hiveClient.close();
  }

  public void dropTables(String database, String... tables) throws IOException {
    HiveSyncConfig hiveSyncConfig = hiveSyncConf();
    HoodieHiveClient hiveClient = hiveClient(hiveSyncConfig);
    for (String table : tables) {
      hiveClient.updateHiveSQL("drop table if exists " + database + "." + table);
    }
    hiveClient.close();
  }

  public void dropDatabases(String... databases) throws IOException {
    HiveSyncConfig hiveSyncConfig = hiveSyncConf();
    HoodieHiveClient hiveClient = hiveClient(hiveSyncConfig);
    for (String database : databases) {
      hiveClient.updateHiveSQL("drop database if exists " + database);
    }
    hiveClient.close();
  }

  public void logHiveTable(HiveSyncConfig hiveSyncConfig) {
    logHiveTable(hiveSyncConfig.databaseName, hiveSyncConfig.tableName);
  }

  public void logHiveTable(String databaseName, String... tableNames) {
    for (String tableName : tableNames) {
      createdHiveTableFullNames.add(databaseName + "." + tableName);
    }
  }

  @BeforeEach
  public synchronized void runBeforeEach() throws IOException, InterruptedException {
    initialized = hiveTestService != null && zookeeperTestService != null;
    if (!initialized) {
      hadoopConf = new Configuration();
      zookeeperTestService = new ZookeeperTestService(hadoopConf);
      zookeeperTestService.start();
      hiveTestService = new HiveTestService(hadoopConf);
      hiveTestService.start();
    }
  }

  @AfterEach
  public void cleanUpCreatedHiveTablesAndClients() throws IOException {
    HoodieHiveClient hiveClient = hiveClient(hiveSyncConf());
    Set<String> fullNames = new HashSet<>(createdHiveTableFullNames);
    for (String fullName : fullNames) {
      hiveClient.updateHiveSQL("drop table if exists " + fullName);
    }
    for (HoodieHiveClient client : createdHiveClients) {
      client.close();
    }
    createdHiveTableFullNames.clear();
    createdHiveClients.clear();
  }

  @AfterAll
  public static synchronized void cleanUpAfterAll() {
    if (hiveTestService != null) {
      hiveTestService.stop();
      hiveTestService = null;
    }
    if (zookeeperTestService != null) {
      zookeeperTestService.stop();
      zookeeperTestService = null;
    }
    if (hadoopConf != null) {
      hadoopConf.clear();
      hadoopConf = null;
    }
  }
}
