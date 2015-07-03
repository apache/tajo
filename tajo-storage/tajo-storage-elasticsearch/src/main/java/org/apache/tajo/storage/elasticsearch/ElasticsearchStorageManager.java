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

import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.storage.StorageManager;
import org.apache.tajo.storage.StorageProperty;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.fragment.Fragment;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.List;

public class ElasticsearchStorageManager extends StorageManager {

  public ElasticsearchStorageManager(CatalogProtos.StoreType storeType) {
    super(storeType);
  }

  @Override
  protected void storageInit() throws IOException {

  }

  @Override
  public void createTable(TableDesc tableDesc, boolean ifNotExists) throws IOException {

  }

  @Override
  public void purgeTable(TableDesc tableDesc) throws IOException {

  }

  @Override
  public List<Fragment> getSplits(String fragmentId, TableDesc tableDesc, ScanNode scanNode) throws IOException {
    return null;
  }

  @Override
  public List<Fragment> getNonForwardSplit(TableDesc tableDesc, int currentPage, int numFragments) throws IOException {
    return null;
  }

  @Override
  public StorageProperty getStorageProperty() {
    return null;
  }

  @Override
  public void closeStorageManager() {

  }

  @Override
  public TupleRange[] getInsertSortRanges(OverridableConf queryContext, TableDesc tableDesc, Schema inputSchema, SortSpec[] sortSpecs, TupleRange dataRange) throws IOException {
    return new TupleRange[0];
  }

  @Override
  public void beforeInsertOrCATS(LogicalNode node) throws IOException {

  }

  @Override
  public void rollbackOutputCommit(LogicalNode node) throws IOException {

  }

  public Client getClient(ElasticsearchWithOptionInfo optionInfo) {
    return null;
  }
}
