/**
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

package org.apache.tajo.storage.elasticsearch;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.*;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticSearchFileAppender extends FileAppender {
  private static final Log LOG = LogFactory.getLog(ElasticSearchFileAppender.class);

  private TableMeta meta;
  private Schema schema;
  private TableStatistics stats = null;

  private int columnNum;
  private byte[] nullChars;

  private Settings settings;
  private Client client;
  private BulkRequestBuilder bulkRequest;

  private String clusterName;
  private String[] nodes;
  private String index;
  private String type;
  private int replica;
  private long bulkItemCount, bulkItemSize;

  private int updateRetryCount = 0;

  public ElasticSearchFileAppender(Configuration conf, Schema schema, TableMeta meta, Path path)
    throws IOException {
    super(conf, schema, meta, path);
    this.meta = meta;
    this.schema = schema;
  }

  @Override
  public void init() throws IOException {
    this.columnNum = schema.size();
    String nullCharacters = StringEscapeUtils.unescapeJava(this.meta.getOption(
      StorageConstants.ELASTICSEARCH_NULL, NullDatum.DEFAULT_TEXT));
    if (StringUtils.isEmpty(nullCharacters)) {
      nullChars = NullDatum.get().asTextBytes();
    } else {
      nullChars = nullCharacters.getBytes();
    }

    clusterName = this.meta.getOption(StorageConstants.ELASTICSEARCH_CLUSTER, "elasticsearch");
    nodes = this.meta.getOption(StorageConstants.ELASTICSEARCH_NODES).split(",");
    index = this.meta.getOption(StorageConstants.ELASTICSEARCH_INDEX);
    type = this.meta.getOption(StorageConstants.ELASTICSEARCH_TYPE);

    bulkItemSize = this.meta.getOptions().getLong(StorageConstants.ELASTICSEARCH_BULK_ITEM_SIZE
      , 5000L);

    replica = this.meta.getOptions().getInt(StorageConstants.ELASTICSEARCH_REPLICATION, 1);

    try {
      settings = StorageUtil.getElasticSearchSettings(clusterName);
      client = StorageUtil.getElasticSearchClient(settings, nodes);
//      client = StorageUtil.getElasticSearchLocalClient(settings);
      bulkRequest = client.prepareBulk();
    } catch (Exception e) {
    } finally {
    }

    this.stats = new TableStatistics(this.schema);
    super.init();
  }

  @Override
  public void addTuple(Tuple tuple) throws IOException {
    XContentBuilder json = jsonBuilder().startObject();

    for (int i = 0; i < columnNum; i++) {
      json.field(schema.getColumn(i).getSimpleName(), tuple.get(i).asChars());
    }
    json.endObject();

    IndexRequest indexRequest = new IndexRequest();
    indexRequest = indexRequest.index(index)
      .type(type)
      .source(json)
      .consistencyLevel(WriteConsistencyLevel.QUORUM)
      .replicationType(ReplicationType.ASYNC)
      .refresh(false);

    // If tajo gather a lot of tokens, tajo will execute BulkRequest. If there remains some tokens,
    // it will be execute at close method. For reference, when you create a table, you can set
    // tokens volume with elasticsearch.bulk.item.size option.
    if (bulkItemCount < bulkItemSize) {
      bulkItemCount++;
      bulkRequest.add (indexRequest);
    } else {
      if ( bulkRequest.numberOfActions() > 0 ) {
        BulkResponse bulkResponse = bulkRequest.setConsistencyLevel(WriteConsistencyLevel.QUORUM)
          .setReplicationType(ReplicationType.ASYNC)
          .setRefresh(false)
          .execute()
          .actionGet();

        client.close();

        try {
          settings = StorageUtil.getElasticSearchSettings(clusterName);
          client = StorageUtil.getElasticSearchClient(settings, nodes);
        } catch (Exception e) {
        }
      }

      bulkItemCount = 0;
      bulkRequest = client.prepareBulk();
      bulkRequest.add (indexRequest);
    }

    stats.incrementRow();
  }

  @Override
  public long getOffset() throws IOException {
    return 0;
  }

  @Override
  public void flush() throws IOException {
  }

  @Override
  public void close() throws IOException {
    if ( bulkRequest.numberOfActions() > 0 ) {
      BulkResponse bulkResponse = bulkRequest.setConsistencyLevel(WriteConsistencyLevel.QUORUM)
        .setReplicationType(ReplicationType.ASYNC)
        .setRefresh(false)
        .execute()
        .actionGet();
    }
    client.close();

// TODO: We need to implement following methods because it will be good for ES performance.
// But current tajo-storage architecture can't adapt these methods.
// Fortunately, this issue is in progress. See TAJO-1122.
//    FlushResponse flush = client.admin().indices()
//      .flush(
//        new FlushRequest()
//          .indices(index)
//          .full(true)
//      ).actionGet();
//
//    RefreshResponse refresh = client.admin().indices()
//      .refresh(
//        new RefreshRequest()
//          .indices(index)
//      ).actionGet();
  }

//  public void beforeAppend() {
//    UpdateSettingsResponse updateSettingsResponse = null;
//
//    try {
//      updateSettingsResponse = client.admin().indices()
//        .prepareUpdateSettings(index)
//        .setMasterNodeTimeout("90s")
//        .setTimeout("90s")
//        .setSettings("{\"index\" : {\"number_of_replicas\" : 0, \"refresh_interval\" : \"-1\" } }")
//        .execute()
//        .actionGet("90s");
//    } catch (Exception e) {
//    }
//
//    // retry three times
//    if ( !updateSettingsResponse.isAcknowledged() && updateRetryCount < 2 ) {
//      updateRetryCount++;
//      initElasticSearchIndex();
//    } else {
//      updateRetryCount = 0;
//    }
//  }
//
//  public void afterAppend() {
//    UpdateSettingsResponse updateSettingsResponse = null;
//
//    try {
//      updateSettingsResponse = client.admin().indices()
//        .prepareUpdateSettings(index)
//        .setMasterNodeTimeout("90s")
//        .setTimeout("90s")
//        .setSettings("{\"index\" : {\"number_of_replicas\" : "+replica
//          +", \"refresh_interval\" : \"1h\" } }")
//        .execute()
//        .actionGet("90s");
//    } catch (Exception e) {
//    } finally {
//    }
//
//    // retry three times
//    if ( !updateSettingsResponse.isAcknowledged() && updateRetryCount < 2 ) {
//      updateRetryCount++;
//      closeElasticSearchIndex();
//    } else {
//      updateRetryCount = 0;
//    }
//  }

  @Override
  public TableStats getStats() {
    return stats.getTableStat();
  }
}
