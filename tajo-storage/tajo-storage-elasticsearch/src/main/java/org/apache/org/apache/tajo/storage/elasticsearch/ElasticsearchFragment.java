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

package org.apache.org.apache.tajo.storage.elasticsearch;

import com.google.gson.annotations.Expose;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.storage.elasticsearch.StorageFragmentProtos.*;
import org.apache.tajo.storage.fragment.Fragment;

/**
 * Created by hwjeong on 15. 3. 18..
 */
public class ElasticsearchFragment implements Fragment, Comparable<ElasticsearchFragment>, Cloneable {
  private static final Log LOG = LogFactory.getLog(ElasticsearchFragment.class);

  @Expose
  private String tableName;
  @Expose
  private String indexName;
  @Expose
  private String indexType;
  @Expose
  private String nodes;
  @Expose
  private int shardId;
  @Expose
  private int offset = -1;
  @Expose
  private int fetchSize;
  @Expose
  private long length;

  public ElasticsearchFragment(ByteString raw) throws InvalidProtocolBufferException {
    ElasticsearchFragmentProto.Builder builder = ElasticsearchFragmentProto.newBuilder();
    builder.mergeFrom(raw);
    builder.build();
    init(builder.build());
  }

  public ElasticsearchFragment(String tableName, String indexName, String indexType, String nodes, int shardId) {
    this.set(tableName, indexName, indexType, nodes, shardId, 0, Integer.parseInt(ElasticsearchConstants.FETCH_SIZE), TajoConstants.UNKNOWN_LENGTH);
  }

  public ElasticsearchFragment(String tableName, String indexName, String indexType, String nodes, int shardId, int offset) {
    this.set(tableName, indexName, indexType, nodes, shardId, offset, Integer.parseInt(ElasticsearchConstants.FETCH_SIZE), TajoConstants.UNKNOWN_LENGTH);
  }

  public ElasticsearchFragment(String tableName, String indexName, String indexType, String nodes, int shardId, int offset, int fetchSize) {
    this.set(tableName, indexName, indexType, nodes, shardId, offset, fetchSize, TajoConstants.UNKNOWN_LENGTH);
  }

  private void set(String tableName, String indexName, String indexType, String nodes, int shardId, int offset, int fetchSize, long length) {
    this.tableName = tableName;
    this.indexName = indexName;
    this.indexType = indexType;
    this.nodes = nodes;
    this.shardId = shardId;
    this.offset = offset;
    this.fetchSize = fetchSize;
    this.length = length;
  }

  private void init(ElasticsearchFragmentProto proto) {
    this.tableName = proto.getTableName();
    this.indexName = proto.getIndexName();
    this.indexType = proto.getIndexType();
    this.nodes = proto.getNodes();
    this.shardId = proto.getShardId();
    this.offset = proto.getOffset();
    this.fetchSize = proto.getFetchSize();
    this.length = proto.getLength();
  }

  @Override
  public int compareTo(ElasticsearchFragment o) {
    if ( shardId == o.shardId ) {
      return 1;
    } else {
      return -1;
    }
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getIndexType() {
    return indexType;
  }

  public String getNodes() {
    return nodes;
  }

  public int getShardId() {
    return shardId;
  }

  public int getOffset() {
    return offset;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public void setLength(long length) {
    this.length = length;
  }

  @Override
  public CatalogProtos.FragmentProto getProto() {
    ElasticsearchFragmentProto.Builder builder = ElasticsearchFragmentProto.newBuilder();
    builder.setTableName(tableName)
      .setIndexName(indexName)
      .setIndexType(indexType)
      .setNodes(nodes)
      .setShardId(shardId)
      .setOffset(offset)
      .setFetchSize(fetchSize)
      .setLength(length);

    FragmentProto.Builder fragmentBuilder = FragmentProto.newBuilder();
    fragmentBuilder.setId(this.tableName);
    fragmentBuilder.setContents(builder.buildPartial().toByteString());
    fragmentBuilder.setStoreType(CatalogUtil.getStoreTypeString(StoreType.ELASTICSEARCH));

    return fragmentBuilder.build();
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public String getKey() {
    return null;
  }

  @Override
  public String[] getHosts() {
    return new String[] {nodes};
  }

  @Override
  public boolean isEmpty() {
    return (offset == -1);
  }
}
