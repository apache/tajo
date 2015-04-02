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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import net.minidev.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.json.JsonLineDeserializer;
import org.apache.tajo.storage.text.TextLineDeserializer;
import org.apache.tajo.storage.text.TextLineParsingError;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ElasticsearchScanner implements Scanner {
  private static final Log LOG = LogFactory.getLog(ElasticsearchScanner.class);

  private TajoConf conf;
  private Schema schema;
  private TableMeta tableMeta;
  private ElasticsearchFragment fragment;
  private ElasticsearchWithOptionInfo optionInfo;
  private TableStats tableStats;
  private Client client;

  protected boolean inited = false;
  private AtomicBoolean finished = new AtomicBoolean(false);
  private float progress = 0.0f;

  private Column[] columns;
  private int[] columnIdxs;
  private TextLineDeserializer deserializer;
  private List<JSONObject> docs = null;
  private int docCount = 0;
  private boolean scanFlag = true;

  public ElasticsearchScanner (Configuration conf, Schema schema, TableMeta tableMeta, Fragment fragment) throws IOException {
    this.conf = (TajoConf)conf;
    this.schema = schema;
    this.tableMeta = tableMeta;
    this.fragment = (ElasticsearchFragment)fragment;
    this.tableStats = new TableStats();
    this.optionInfo = new ElasticsearchWithOptionInfo(tableMeta);
    this.client = null;
  }

  @Override
  public void init() throws IOException {
    docs = new ArrayList<JSONObject>();
    inited = true;

    if (columns == null) {
      columns = schema.toArray();
    }

    columnIdxs = new int[columns.length];

    for (int i = 0; i < columns.length; i++) {
      columnIdxs[i] = schema.getColumnId(columns[i].getQualifiedName());
    }

    Arrays.sort(columnIdxs);

    deserializer = new JsonLineDeserializer(schema, tableMeta, columnIdxs);
    deserializer.init();

    documentScanner();

  }

  public void documentScanner () {
    String indexName = fragment.getIndexName();
    String indexType = fragment.getIndexType();
    int offset = fragment.getOffset();
    int fetchSize = fragment.getFetchSize();
    int shardId = fragment.getShardId();

    try {
      if ( client == null ) {
        client = ((ElasticsearchStorageManager) StorageManager.getStorageManager(conf, CatalogProtos.StoreType.ELASTICSEARCH)).getClient(optionInfo);
      }

      SearchResponse res = client.prepareSearch(optionInfo.index())
          .setTypes(optionInfo.type())
          .setSearchType(SearchType.SCAN)
          .setPreference("_shards:" + shardId + ";_primary")
          .setQuery(new MatchAllQueryBuilder())   // if it can receive a qual, query have to generate to query dsl.
          .setFrom(offset)
          .setSize(fetchSize)
          .setScroll(optionInfo.timeScroll())
          .execute()
          .actionGet(optionInfo.timeAction());

      while ( true ) {
        res = client.prepareSearchScroll(res.getScrollId())
            .setScroll(optionInfo.timeScroll())
            .execute()
            .actionGet(optionInfo.timeAction());

        for ( SearchHit doc : res.getHits().getHits() ) { // we replace to tajo data format which is a depth json line.
          doc.sourceAsMap().put(ElasticsearchConstants.GLOBAL_FIELDS_TYPE, doc.getType());
          doc.sourceAsMap().put(ElasticsearchConstants.GLOBAL_FIELDS_SCORE, doc.getScore());
          doc.sourceAsMap().put(ElasticsearchConstants.GLOBAL_FIELDS_ID, doc.getId());

          docs.add(new JSONObject(doc.sourceAsMap()));
        }

        if (res.getHits().hits().length == 0) {
          ClearScrollRequest csReq = new ClearScrollRequest();
          csReq.addScrollId(res.getScrollId());

          client.clearScroll(csReq).actionGet();
          break;
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
    }
  }

  public void setClient(Client client) {
    this.client = client;
  }

  @Override
  public Tuple next() throws IOException {
    if (finished.get()) {
      return null;
    }

    if(null == docs) {
      return null;
    }

    if( docs.size() <= docCount) {
      finished.set(true);
      progress = 1.0f;

      return null;
    }

    VTuple tuple;
    JSONObject doc = docs.get(docCount);
    ByteBuf buf = Unpooled.wrappedBuffer(doc.toString().getBytes(ElasticsearchConstants.CHARSET));

    if (buf == null) {
      return null;
    }

    if (columns.length == 0) {
      docCount++;
      return EmptyTuple.get();
    }

    tuple = new VTuple(schema.size());

    try {
      deserializer.deserialize(buf, tuple);
    } catch (TextLineParsingError tae) {
      throw new IOException(tae);
    } finally {
      docCount++;
    }

    return tuple;
  }

  @Override
  public void reset() throws IOException {
    progress = 0.0f;
    docCount = 0;
    finished.set(false);
  }

  @Override
  public void close() throws IOException {
    progress = 1.0f;
    finished.set(true);
    docs = null;

    if (tableStats != null) {
      tableStats.setNumRows(docCount);
    }
  }

  @Override
  public boolean isProjectable() {
    return false;
  }

  @Override
  public void setTarget(Column[] targets) {
    if (inited) {
      throw new IllegalStateException("Should be called before init()");
    }

    this.columns = targets;
  }

  @Override
  public boolean isSelectable() {
    return false;
  }

  @Override
  public void setSearchCondition(Object expr) {

  }

  @Override
  public boolean isSplittable() {
    return false;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public TableStats getInputStats() {
    if (tableStats != null) {
      tableStats.setNumRows(docCount);
    }

    return tableStats;
  }

  @Override
  public Schema getSchema() {
    return null;
  }
}
