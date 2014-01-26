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

/**
 * 
 */
package org.apache.tajo.catalog.store;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MemStore implements CatalogStore {
  private final Map<String,CatalogProtos.TableDescProto> tables = Maps.newHashMap();
  private final Map<String, CatalogProtos.FunctionDescProto> functions = Maps.newHashMap();
  private final Map<String, IndexDescProto> indexes = Maps.newHashMap();
  private final Map<String, IndexDescProto> indexesByColumn = Maps.newHashMap();
  
  public MemStore(Configuration conf) {
  }

  /* (non-Javadoc)
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    tables.clear();
    functions.clear();
    indexes.clear();
  }

  /* (non-Javadoc)
   * @see CatalogStore#addTable(TableDesc)
   */
  @Override
  public void addTable(CatalogProtos.TableDescProto desc) throws IOException {
    synchronized(tables) {
      String tableId = desc.getId().toLowerCase();
      tables.put(tableId, desc);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#existTable(java.lang.String)
   */
  @Override
  public boolean existTable(String name) throws IOException {
    synchronized(tables) {
      String tableId = name.toLowerCase();
      return tables.containsKey(tableId);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#deleteTable(java.lang.String)
   */
  @Override
  public void deleteTable(String name) throws IOException {
    synchronized(tables) {
      String tableId = name.toLowerCase();
      tables.remove(tableId);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#getTable(java.lang.String)
   */
  @Override
  public CatalogProtos.TableDescProto getTable(String name) throws IOException {
    String tableId = name.toLowerCase();
    CatalogProtos.TableDescProto unqualified = tables.get(tableId);
    if(unqualified == null)
      return null;
    CatalogProtos.TableDescProto.Builder builder = CatalogProtos.TableDescProto.newBuilder();
    CatalogProtos.SchemaProto schemaProto = CatalogUtil.getQualfiedSchema(tableId, unqualified.getSchema());
    builder.mergeFrom(unqualified);
    builder.setSchema(schemaProto);
    return builder.build();
  }

  /* (non-Javadoc)
   * @see CatalogStore#getAllTableNames()
   */
  @Override
  public List<String> getAllTableNames() throws IOException {
    return new ArrayList<String>(tables.keySet());
  }

  @Override
  public void addPartitionMethod(CatalogProtos.PartitionMethodProto partitionMethodProto) throws IOException {
    throw new IOException("not supported!");
  }

  @Override
  public CatalogProtos.PartitionMethodProto getPartitionMethod(String tableName) throws IOException {
    String tableId = tableName.toLowerCase();
    CatalogProtos.TableDescProto table = tables.get(tableId);
    return (table != null && table.hasPartition()) ? table.getPartition() : null;
  }

  @Override
  public boolean existPartitionMethod(String tableName) throws IOException {
    String tableId = tableName.toLowerCase();
    CatalogProtos.TableDescProto table = tables.get(tableId);
    return (table != null && table.hasPartition());
  }

  @Override
  public void delPartitionMethod(String tableName) throws IOException {
    throw new IOException("not supported!");
  }

  @Override
  public void addPartitions(CatalogProtos.PartitionsProto partitionDescList) throws IOException {
    throw new IOException("not supported!");
  }

  @Override
  public void addPartition(CatalogProtos.PartitionDescProto partitionDesc) throws IOException {
    throw new IOException("not supported!");
  }

  @Override
  public CatalogProtos.PartitionsProto getPartitions(String tableName) throws IOException {
    throw new IOException("not supported!");
  }

  @Override
  public CatalogProtos.PartitionDescProto getPartition(String partitionName) throws IOException {
    throw new IOException("not supported!");
  }

  @Override
  public void delPartition(String partitionName) throws IOException {
    throw new IOException("not supported!");
  }

  @Override
  public void delPartitions(String tableName) throws IOException {
    throw new IOException("not supported!");
  }

  /* (non-Javadoc)
   * @see CatalogStore#addIndex(nta.catalog.proto.CatalogProtos.IndexDescProto)
   */
  @Override
  public void addIndex(IndexDescProto proto) throws IOException {
    synchronized(indexes) {
      indexes.put(proto.getName(), proto);
      indexesByColumn.put(proto.getTableId() + "." 
          + proto.getColumn().getColumnName(), proto);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#delIndex(java.lang.String)
   */
  @Override
  public void delIndex(String indexName) throws IOException {
    synchronized(indexes) {
      indexes.remove(indexName);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#getIndex(java.lang.String)
   */
  @Override
  public IndexDescProto getIndex(String indexName) throws IOException {
    return indexes.get(indexName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#getIndex(java.lang.String, java.lang.String)
   */
  @Override
  public IndexDescProto getIndex(String tableName, String columnName)
      throws IOException {
    return indexesByColumn.get(tableName+"."+columnName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#existIndex(java.lang.String)
   */
  @Override
  public boolean existIndex(String indexName) throws IOException {
    return indexes.containsKey(indexName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#existIndex(java.lang.String, java.lang.String)
   */
  @Override
  public boolean existIndex(String tableName, String columnName)
      throws IOException {
    return indexesByColumn.containsKey(tableName + "." + columnName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#getIndexes(java.lang.String)
   */
  @Override
  public IndexDescProto[] getIndexes(String tableName) throws IOException {
    List<IndexDescProto> protos = new ArrayList<IndexDescProto>();
    for (IndexDescProto proto : indexesByColumn.values()) {
      if (proto.getTableId().equals(tableName)) {
        protos.add(proto);
      }
    }
    return protos.toArray(new IndexDescProto[protos.size()]);
  }

  /* (non-Javadoc)
   * @see CatalogStore#addFunction(FunctionDesc)
   */
  @Override
  public void addFunction(FunctionDesc func) throws IOException {
    // to be implemented
  }

  /* (non-Javadoc)
   * @see CatalogStore#deleteFunction(FunctionDesc)
   */
  @Override
  public void deleteFunction(FunctionDesc func) throws IOException {
    // to be implemented
  }

  /* (non-Javadoc)
   * @see CatalogStore#existFunction(FunctionDesc)
   */
  @Override
  public void existFunction(FunctionDesc func) throws IOException {
    // to be implemented
  }

  /* (non-Javadoc)
   * @see CatalogStore#getAllFunctionNames()
   */
  @Override
  public List<String> getAllFunctionNames() throws IOException {
    // to be implemented
    return null;
  }

}
