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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.action.admin.indices.status.IndicesStatusResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class TestElasticSearchAppender {

  private TajoConf conf;
  private final static String TEST_PATH = "target/test-data/TestElasticSearchAppender";
  private static String clusterName = "elasticsearch";
  private Path workDir;

  private Settings settings;
  private Node node;
  private Client client;
  private String address;
  private int port;

  @Before
  public void setup() throws Exception {
    conf = new TajoConf();
    workDir = CommonTestingUtil.getTestDir(TEST_PATH);

    settings = ImmutableSettings.settingsBuilder()
      .put("path.data", workDir.toUri().getPath().toString())
      .put("cluster.name", clusterName)
      .build();

    node = NodeBuilder.nodeBuilder().local(false).settings(settings).build();
    client = node.client();

    node.start();

    TransportAddress transportAddress = ((InternalNode) node).injector().getInstance
      (TransportService.class).boundAddress().publishAddress();

    InetSocketTransportAddress socketAddress = (InetSocketTransportAddress) transportAddress;
    address = socketAddress.address().getAddress().getHostAddress();
    port = socketAddress.address().getPort();
  }

  @After
  public void tearDown() throws Exception {
    client.close();
    node.stop();
    CommonTestingUtil.cleanupTestDir(TEST_PATH);
  }

  @Test
  public void testElasticSearchAppender() throws Exception {
    String index = "test";
    String type = "table1";

    createElasticSearchIndex(index, type);

    createTajoTable(index, type);

    verifyLoadedData(index, type);
  }

  private void createElasticSearchIndex(String index, String type) throws Exception {
    // Create the index
    client.admin().indices().prepareCreate(index).addMapping(type,
      getMappingForType(type)).execute().actionGet();

    client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

    // Check the index status
    IndicesStatusResponse status = client.admin().indices().prepareStatus().execute().actionGet();
    assertNotNull(status);
    assertEquals(status.getIndices().size(), 1);
    assertNotNull(status.getIndex(index));

    // Create mapping fields for the index
    GetFieldMappingsResponse response = client.admin().indices().prepareGetFieldMappings()
      .setFields("id", "name", "score", "type").includeDefaults(true).get();

    // Check mapping fields status
    assertEquals(response.mappings().size(), 1);

    assertThat((Map<String, Object>) response.fieldMappings("test", type,
      "id").sourceAsMap().get("id"), hasEntry("index", (Object) "not_analyzed"));

    assertThat((Map<String, Object>) response.fieldMappings("test", type,
      "id").sourceAsMap().get("id"), hasEntry("type", (Object) "integer"));

    assertThat((Map<String, Object>) response.fieldMappings("test", type,
      "name").sourceAsMap().get("name"), hasEntry("index", (Object) "not_analyzed"));

    assertThat((Map<String, Object>) response.fieldMappings("test", type,
      "name").sourceAsMap().get("name"), hasEntry("type", (Object) "string"));

    assertThat((Map<String, Object>) response.fieldMappings("test", type,
      "score").sourceAsMap().get("score"), hasEntry("index", (Object) "no"));

    assertThat((Map<String, Object>) response.fieldMappings("test", type,
      "score").sourceAsMap().get("score"), hasEntry("type", (Object) "float"));

    assertThat((Map<String, Object>) response.fieldMappings("test", type,
      "type").sourceAsMap().get("type"), hasEntry("index", (Object) "no"));

    assertThat((Map<String, Object>) response.fieldMappings("test", type,
      "type").sourceAsMap().get("type"), hasEntry("type", (Object) "string"));
  }

  private XContentBuilder getMappingForType(String type) throws IOException {
    return jsonBuilder().startObject().startObject(type).startObject("properties")

      .startObject("id").field("type", "integer").field("store", "yes")
      .field("index", "not_analyzed").field("index_options","docs")
      .field("ignore_malformed", "true").field("include_in_all", "false").endObject()

      .startObject("name").field("type", "string").field("store", "yes")
      .field("index", "not_analyzed").field("index_options","docs")
      .field("ignore_malformed", "true").field("include_in_all", "false").endObject()

      .startObject("score").field("type", "float").field("store", "yes")
      .field("index", "no").field("index_options","docs")
      .field("ignore_malformed", "true").field("include_in_all", "false").endObject()

      .startObject("type").field("type", "string").field("store", "yes")
      .field("index", "no").field("index_options","docs")
      .field("ignore_malformed", "true").field("include_in_all", "false").endObject()

      .endObject().endObject().endObject();
  }


  private void createTajoTable(String index, String type) throws Exception {
    KeyValueSet options = new KeyValueSet();

    options.set(StorageConstants.ELASTICSEARCH_CLUSTER, clusterName);
    options.set(StorageConstants.ELASTICSEARCH_NODES, address + ":" + port);
    options.set(StorageConstants.ELASTICSEARCH_INDEX, index);
    options.set(StorageConstants.ELASTICSEARCH_TYPE, type);

    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.ELASTICSEARCH);
    meta.setOptions(options);

    Path tablePath = new Path(workDir, "table1.data");
    Schema schema = new Schema();

    schema.addColumn("id", TajoDataTypes.Type.INT4);
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    schema.addColumn("score", TajoDataTypes.Type.FLOAT4);
    schema.addColumn("type", TajoDataTypes.Type.TEXT);

    Appender appender = StorageManager.getStorageManager(conf).getAppender(meta, schema, tablePath);
    appender.enableStats();
    appender.init();

    VTuple vTuple;

    int tupleNum = 20;
    int cnt = 0;
    for (int i = 0; i < tupleNum; i++) {
      cnt = i +1;
      vTuple = new VTuple(4);
      vTuple.put(0, DatumFactory.createInt4(cnt));
      vTuple.put(1, DatumFactory.createText("name_" + cnt));
      vTuple.put(2, DatumFactory.createFloat4(cnt * 0.1F));
      vTuple.put(3, DatumFactory.createText("type_" + cnt));
      appender.addTuple(vTuple);
    }
    appender.close();
    TableStats stat = appender.getStats();
    assertEquals(tupleNum, stat.getNumRows().longValue());

    RefreshRequestBuilder refreshRequestBuilder = new RefreshRequestBuilder(client.admin().indices());
    refreshRequestBuilder.setIndices(index).execute().actionGet();
  }

  private void verifyLoadedData(String index, String type) {

    SearchResponse searchResponse = client.prepareSearch(index)
      .setQuery(QueryBuilders.rangeQuery("id").gte(6).lte(10))
      .execute().actionGet();

    assertEquals(searchResponse.getHits().getTotalHits(), 5L);

    searchResponse = client.prepareSearch(index)
      .setTypes(type)
      .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
      .execute()
      .actionGet();

    assertEquals(searchResponse.getHits().getTotalHits(), 20L);
  }

}
