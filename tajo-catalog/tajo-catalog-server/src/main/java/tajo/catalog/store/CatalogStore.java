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

package tajo.catalog.store;

import tajo.catalog.FunctionDesc;
import tajo.catalog.TableDesc;
import tajo.catalog.proto.CatalogProtos.IndexDescProto;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface CatalogStore extends Closeable {
  void addTable(TableDesc desc) throws IOException;
  
  boolean existTable(String name) throws IOException;
  
  void deleteTable(String name) throws IOException;
  
  TableDesc getTable(String name) throws IOException;
  
  List<String> getAllTableNames() throws IOException;
  
  void addIndex(IndexDescProto proto) throws IOException;
  
  void delIndex(String indexName) throws IOException;
  
  IndexDescProto getIndex(String indexName) throws IOException;
  
  IndexDescProto getIndex(String tableName, String columnName) 
      throws IOException;
  
  boolean existIndex(String indexName) throws IOException;
  
  boolean existIndex(String tableName, String columnName) throws IOException;
  
  IndexDescProto [] getIndexes(String tableName) throws IOException;
  
  void addFunction(FunctionDesc func) throws IOException;
  
  void deleteFunction(FunctionDesc func) throws IOException;
  
  void existFunction(FunctionDesc func) throws IOException;
  
  List<String> getAllFunctionNames() throws IOException;
}
