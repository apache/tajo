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

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.KeyValueSet;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestElasticsearchScanner {
  private ImmutableSettings.Builder settings;
  private Node node;
  private Client client;
  private StoreType storeType;
  private TajoConf conf;
  private Schema schema;
  private TableMeta tableMeta;
  private ElasticsearchFragment fragment;
  private KeyValueSet options;

  public TestElasticsearchScanner () {
    this.storeType = CatalogProtos.StoreType.ELASTICSEARCH;
  }

  @Before
  public void setup() throws Exception {
    settings = ImmutableSettings.settingsBuilder();
    settings.put("cluster.name", "testTajoCluster");
    settings.put("node.name", "testTajoNode");
    settings.put("path.data", "data/index");    // path.data create a relative path which is located in tajo-storage-elasticsearch/data/index
    settings.put("gateway.type", "none");

    node = NodeBuilder.nodeBuilder()
        .settings(settings)
        .data(true)
        .local(false)
        .node();

    client = node.client();
  }

  @Test
  public void testElasticsearchScanner() throws Exception {
    // delete index
    try {
      client.admin().indices().prepareDelete("test-ndex").execute().actionGet();
    } catch (Exception e) {
    } finally {
    }

    // create index
    Settings indexSettings = ImmutableSettings.settingsBuilder()
        .put("number_of_shards","1")
        .put("number_of_replicas", "0")
        .build();

    XContentBuilder builder = XContentFactory.jsonBuilder()
      .startObject()
        .startObject("test-type")
          .startObject("_all")
            .field("enabled", "false")
          .endObject()
          .startObject("_id")
            .field("path", "field1")
          .endObject()
          .startObject("properties")
            .startObject("field1")
              .field("type", "long").field("store", "no").field("index", "not_analyzed")
            .endObject()
            .startObject("field2")
              .field("type", "string").field("store", "no").field("index", "not_analyzed")
            .endObject()
            .startObject("field3")
              .field("type", "string").field("store", "no").field("index", "analyzed")
            .endObject()
          .endObject()
        .endObject()
      .endObject();

    CreateIndexResponse res = client.admin().indices().prepareCreate("test-index")
        .setSettings(indexSettings)
        .addMapping("test-type", builder)
        .execute()
        .actionGet();

    assertEquals(res.isAcknowledged(), true);

    // add document
    IndexRequestBuilder indexRequestBuilder = client.prepareIndex().setIndex("test-index").setType("test-type");
    IndexResponse indexResponse;

    for ( int i=0; i<10; i++ ) {
      builder = XContentFactory.jsonBuilder()
        .startObject()
          .field("field1", i).field("field2", "henry" + i).field("field3", i + ". hello world!! elasticsearch on apache tajo!!")
        .endObject();

      indexResponse = indexRequestBuilder.setSource(builder)
          .setId(String.valueOf(i))
          .setOperationThreaded(false)
          .setConsistencyLevel(WriteConsistencyLevel.QUORUM)
          .setReplicationType(ReplicationType.ASYNC)
          .execute()
          .actionGet();

      assertEquals(indexResponse.isCreated(), true);
    }

    client.admin().indices().prepareRefresh("test-index").execute().actionGet();

    // run scanner
    conf = new TajoConf();

    schema = new Schema();
    schema.addColumn("_type", TajoDataTypes.Type.TEXT);
    schema.addColumn("_score", TajoDataTypes.Type.FLOAT8);
    schema.addColumn("_id", TajoDataTypes.Type.TEXT);
    schema.addColumn("field1", TajoDataTypes.Type.INT8);
    schema.addColumn("field2", TajoDataTypes.Type.TEXT);
    schema.addColumn("field3", TajoDataTypes.Type.TEXT);

    Schema targetSchema = new Schema();
    targetSchema.addColumn(schema.getColumn(0));
    targetSchema.addColumn(schema.getColumn(1));
    targetSchema.addColumn(schema.getColumn(2));
    targetSchema.addColumn(schema.getColumn(3));
    targetSchema.addColumn(schema.getColumn(4));
    targetSchema.addColumn(schema.getColumn(5));

    options = new KeyValueSet();
    options.set("es.cluster", "testTajoCluster");
    options.set("es.nodes", "localhost:9300");
    options.set("es.index", "test-index");
    options.set("es.type", "test-type");

    tableMeta = CatalogUtil.newTableMeta(storeType, options);

    fragment = new ElasticsearchFragment();
    fragment.setIndexName("test-index");
    fragment.setIndexType("test-type");
    fragment.setOffset(0);
    fragment.setFetchSize(10);
    fragment.setShardId(0);

    ElasticsearchScanner scanner = new ElasticsearchScanner(conf, targetSchema, tableMeta, fragment);

    scanner.setClient(client);
    scanner.init();

    int totalCounts = 0;
    Tuple tuple;
    while ((tuple = scanner.next()) != null) {
      assertEquals("henry"+totalCounts, tuple.getText(4));
      totalCounts++;
    }

    scanner.close();

    assertEquals(totalCounts, 10);
  }

  @After
  public void tearDown() throws Exception {
    client.close();
    node.close();
  }

  private static boolean isProjectableStorage(StoreType type) {
    switch (type) {
      case RCFILE:
      case PARQUET:
      case SEQUENCEFILE:
      case CSV:
      case AVRO:
      case ELASTICSEARCH:
        return true;
      default:
        return false;
    }
  }
}