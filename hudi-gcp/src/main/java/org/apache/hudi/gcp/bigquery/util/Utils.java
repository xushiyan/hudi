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

package org.apache.hudi.gcp.bigquery.util;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.gcp.bigquery.BigQuerySyncConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class Utils {
  public static String BIGQUERY_SYNC_PROJECT_ID = "hoodie.gcp.bigquery.sync.project_id";
  public static String BIGQUERY_SYNC_DATASET_NAME = "hoodie.gcp.bigquery.sync.dataset_name";
  public static String BIGQUERY_SYNC_DATASET_LOCATION = "hoodie.gcp.bigquery.sync.dataset_location";
  public static String BIGQUERY_SYNC_TABLE_NAME = "hoodie.gcp.bigquery.sync.table_name";
  public static String BIGQUERY_SYNC_SOURCE_URI = "hoodie.gcp.bigquery.sync.source_uri";
  public static String BIGQUERY_SYNC_SOURCE_URI_PREFIX = "hoodie.gcp.bigquery.sync.source_uri_prefix";
  public static String BIGQUERY_SYNC_SYNC_BASE_PATH = "hoodie.gcp.bigquery.sync.base_path";
  public static String BIGQUERY_SYNC_PARTITION_FIELDS = "hoodie.gcp.bigquery.sync.partition_fields";
  public static String BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA = "hoodie.gcp.bigquery.sync.use_file_listing_from_metadata";
  public static String BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING = "hoodie.gcp.bigquery.sync.assume_date_partitioning";

  public static TypedProperties configToProperties(BigQuerySyncConfig cfg) {
    TypedProperties properties = new TypedProperties();
    properties.put(BIGQUERY_SYNC_PROJECT_ID, cfg.projectId);
    properties.put(BIGQUERY_SYNC_DATASET_NAME, cfg.datasetName);
    properties.put(BIGQUERY_SYNC_DATASET_LOCATION, cfg.datasetLocation);
    properties.put(BIGQUERY_SYNC_TABLE_NAME, cfg.tableName);
    properties.put(BIGQUERY_SYNC_SOURCE_URI, cfg.sourceUri);
    properties.put(BIGQUERY_SYNC_SOURCE_URI_PREFIX, cfg.sourceUriPrefix);
    properties.put(BIGQUERY_SYNC_SYNC_BASE_PATH, cfg.basePath);
    properties.put(BIGQUERY_SYNC_PARTITION_FIELDS, cfg.partitionFields);
    properties.put(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA, cfg.useFileListingFromMetadata);
    properties.put(BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING, cfg.assumeDatePartitioning);
    return properties;
  }

  public static BigQuerySyncConfig propertiesToConfig(Properties properties) {
    BigQuerySyncConfig config = new BigQuerySyncConfig();
    config.projectId = properties.getProperty(BIGQUERY_SYNC_PROJECT_ID);
    config.datasetName = properties.getProperty(BIGQUERY_SYNC_DATASET_NAME);
    config.datasetLocation = properties.getProperty(BIGQUERY_SYNC_DATASET_LOCATION);
    config.tableName = properties.getProperty(BIGQUERY_SYNC_TABLE_NAME);
    config.sourceUri = properties.getProperty(BIGQUERY_SYNC_SOURCE_URI);
    config.sourceUriPrefix = properties.getProperty(BIGQUERY_SYNC_SOURCE_URI_PREFIX);
    config.basePath = properties.getProperty(BIGQUERY_SYNC_SYNC_BASE_PATH);
    if (StringUtils.isNullOrEmpty(properties.getProperty(BIGQUERY_SYNC_PARTITION_FIELDS))) {
      config.partitionFields = new ArrayList<>();
    } else {
      config.partitionFields = Arrays.asList(properties.getProperty(BIGQUERY_SYNC_PARTITION_FIELDS).split(","));
    }
    config.useFileListingFromMetadata = Boolean.valueOf(properties.getProperty(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA));
    config.assumeDatePartitioning = Boolean.valueOf(properties.getProperty(BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING));
    return config;
  }
}
