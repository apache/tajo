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

package tajo.catalog;

import tajo.catalog.proto.CatalogProtos.*;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;

public interface CatalogServiceProtocol {
  
  /**
   * Get a table description by name
   * @param name table name
   * @return a table description
   * @see TableDescImpl
   * @throws Throwable
   */
  TableDescProto getTableDesc(StringProto name);
  
  /**
   * 
   * @return
   * @throws tajo.catalog.exception.CatalogException
   */
  GetAllTableNamesResponse getAllTableNames(NullProto request);
  
  /**
   * 
   * @return
   * @throws tajo.catalog.exception.CatalogException
   */
  GetFunctionsResponse getFunctions(NullProto request);
  
  /**
   * Add a table via table description
   * @param meta table meta
   * @see TableDescImpl
   * @throws Throwable
   */
  void addTable(TableDescProto desc);
  
  /**
   * Drop a table by name
   * @param name table name
   * @throws Throwable
   */
  void deleteTable(StringProto name);
  
  BoolProto existsTable(StringProto tableId);
  
  void addIndex(IndexDescProto proto);
  
  BoolProto existIndex(StringProto indexName);
  
  BoolProto existIndex(GetIndexRequest req);
  
  IndexDescProto getIndex(StringProto indexName);
  
  IndexDescProto getIndex(GetIndexRequest req);
  
  void delIndex(StringProto indexName);
  
  /**
   * 
   * @param funcDesc
   */
  void registerFunction(FunctionDescProto funcDesc);
  
  /**
   * 
   * @param signature
   */
  void unregisterFunction(UnregisterFunctionRequest request);
  
  /**
   * 
   * @param signature
   * @return
   */
  FunctionDescProto getFunctionMeta(GetFunctionMetaRequest request);
  
  /**
   * 
   * @param signature
   * @return
   */
  BoolProto containFunction(ContainFunctionRequest request);
}