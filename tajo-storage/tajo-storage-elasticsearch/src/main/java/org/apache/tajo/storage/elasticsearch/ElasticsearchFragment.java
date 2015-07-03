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

import com.google.gson.annotations.Expose;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.storage.fragment.Fragment;

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
  }

  public ElasticsearchFragment() {
  }

  @Override
  public int compareTo(ElasticsearchFragment o) {
    return 0;
  }

  @Override
  public String getTableName() {
    return null;
  }

  @Override
  public CatalogProtos.FragmentProto getProto() {
    return null;
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String getKey() {
    return null;
  }

  @Override
  public String[] getHosts() {
    return new String[0];
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  public String getIndexName() {
    return indexName;
  }

  public String getIndexType() {
    return indexType;
  }

  public int getOffset() {
    return offset;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public int getShardId() {
    return shardId;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public void setIndexType(String indexType) {
    this.indexType = indexType;
  }

  public void setOffset (int offset) {
    this.offset = offset;
  }

  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }

  public void setShardId(int shardId) {
    this.shardId = shardId;
  }
}
