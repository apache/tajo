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

package org.apache.tajo.catalog.store;

import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.IndexDescProto;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface CatalogStore extends Closeable {
  /*************************** TABLE ******************************/
  void addTable(CatalogProtos.TableDescProto desc) throws IOException;
  
  boolean existTable(String name) throws IOException;
  
  void deleteTable(String name) throws IOException;
  
  CatalogProtos.TableDescProto getTable(String name) throws IOException;
  
  List<String> getAllTableNames() throws IOException;


  /************************ PARTITION METHOD **************************/
  void addPartitionMethod(CatalogProtos.PartitionMethodProto partitionMethodProto) throws IOException;

  CatalogProtos.PartitionMethodProto getPartitionMethod(String tableName) throws IOException;

  boolean existPartitionMethod(String tableName) throws IOException;

  void delPartitionMethod(String tableName) throws IOException;


  /************************** PARTITIONS *****************************/
  void addPartitions(CatalogProtos.PartitionsProto partitionsProto) throws IOException;

  void addPartition(CatalogProtos.PartitionDescProto partitionDescProto) throws IOException;

  /**
   * Get all partitions of a table
   * @param tableName the table name
   * @return
   * @throws IOException
   */
  CatalogProtos.PartitionsProto getPartitions(String tableName) throws IOException;

  CatalogProtos.PartitionDescProto getPartition(String partitionName) throws IOException;

  void delPartition(String partitionName) throws IOException;

  void delPartitions(String tableName) throws IOException;

  /**************************** INDEX *******************************/
  void addIndex(IndexDescProto proto) throws IOException;
  
  void delIndex(String indexName) throws IOException;
  
  IndexDescProto getIndex(String indexName) throws IOException;
  
  IndexDescProto getIndex(String tableName, String columnName) 
      throws IOException;
  
  boolean existIndex(String indexName) throws IOException;
  
  boolean existIndex(String tableName, String columnName) throws IOException;

  /************************** FUNCTION *****************************/
  IndexDescProto [] getIndexes(String tableName) throws IOException;
  
  void addFunction(FunctionDesc func) throws IOException;
  
  void deleteFunction(FunctionDesc func) throws IOException;
  
  void existFunction(FunctionDesc func) throws IOException;
  
  List<String> getAllFunctionNames() throws IOException;
}
