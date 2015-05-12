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

package org.apache.tajo.catalog.dictionary;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.catalog.proto.CatalogProtos.TableDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableStatsProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueSetProto;

abstract class AbstractTableDescriptor implements TableDescriptor {
  
  protected InfoSchemaMetadataDictionary dictionary;

  public AbstractTableDescriptor(InfoSchemaMetadataDictionary metadataDictionary) {
    dictionary = metadataDictionary;
  }

  protected abstract ColumnDescriptor[] getColumnDescriptors();
  
  protected SchemaProto getSchemaProto() {
    SchemaProto.Builder schemaBuilder = SchemaProto.newBuilder();
    ColumnProto.Builder columnBuilder = null;
    
    for (ColumnDescriptor columnDescriptor: getColumnDescriptors()) {
      columnBuilder = ColumnProto.newBuilder();
      
      columnBuilder.setName(columnDescriptor.getName().toLowerCase());
      if (columnDescriptor.getLength() > 0) {
        columnBuilder.setDataType(CatalogUtil.newDataTypeWithLen(columnDescriptor.getType(),
            columnDescriptor.getLength()));
      } else {
        columnBuilder.setDataType(CatalogUtil.newSimpleDataType(columnDescriptor.getType()));
      }
      
      schemaBuilder.addFields(columnBuilder.build());
    }
    
    return schemaBuilder.build();
  }
  
  protected TableProto getTableProto() {
    TableProto.Builder metaBuilder = TableProto.newBuilder();
    metaBuilder.setStoreType("SYSTEM");
    metaBuilder.setParams(KeyValueSetProto.newBuilder().build());
    return metaBuilder.build();
  }
  
  protected TableStatsProto getTableStatsProto() {
    TableStatsProto.Builder statBuilder = TableStatsProto.newBuilder();
    statBuilder.setNumRows(0l);
    statBuilder.setNumBytes(0l);
    return statBuilder.build();
  }
  
  @Override
  public TableDescProto getTableDescription() {
    TableDescProto.Builder tableBuilder = TableDescProto.newBuilder();
    
    tableBuilder.setTableName(CatalogUtil.buildFQName(dictionary.getSystemDatabaseName(), getTableNameString()));
    tableBuilder.setPath(dictionary.getTablePath());
    
    tableBuilder.setSchema(CatalogUtil.getQualfiedSchema(
        dictionary.getSystemDatabaseName() + "." + getTableNameString(),
        getSchemaProto()));
    tableBuilder.setMeta(getTableProto());
    tableBuilder.setStats(getTableStatsProto());
    return tableBuilder.build();
  }

}
