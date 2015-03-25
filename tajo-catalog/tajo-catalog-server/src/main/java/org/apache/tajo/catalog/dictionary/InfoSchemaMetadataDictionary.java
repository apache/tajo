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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.tajo.catalog.exception.NoSuchTableException;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.util.TUtil;

public class InfoSchemaMetadataDictionary {
  
  private static final String DATABASE_NAME = "information_schema";
  
  private static enum DEFINED_TABLES {
    TABLESPACES,
    DATABASES,
    TABLES,
    COLUMNS,
    INDEXES,
    TABLEOPTIONS,
    TABLESTATS,
    PARTITIONS,
    PARTITION_KEYS,
    CLUSTER,
    MAX_TABLE;
  }
  
  private List<TableDescriptor> schemaInfoTableDescriptors = new ArrayList<TableDescriptor>(
      Collections.nCopies(DEFINED_TABLES.MAX_TABLE.ordinal(), (TableDescriptor)null));
  
  public InfoSchemaMetadataDictionary() {
    createSystemTableDescriptors();
  }
  
  private void createSystemTableDescriptors() {
    schemaInfoTableDescriptors.set(DEFINED_TABLES.TABLESPACES.ordinal(), new TablespacesTableDescriptor(this));
    schemaInfoTableDescriptors.set(DEFINED_TABLES.DATABASES.ordinal(), new DatabasesTableDescriptor(this));
    schemaInfoTableDescriptors.set(DEFINED_TABLES.TABLES.ordinal(), new TablesTableDescriptor(this));
    schemaInfoTableDescriptors.set(DEFINED_TABLES.COLUMNS.ordinal(), new ColumnsTableDescriptor(this));
    schemaInfoTableDescriptors.set(DEFINED_TABLES.INDEXES.ordinal(), new IndexesTableDescriptor(this));
    schemaInfoTableDescriptors.set(DEFINED_TABLES.TABLEOPTIONS.ordinal(), new TableOptionsTableDescriptor(this));
    schemaInfoTableDescriptors.set(DEFINED_TABLES.TABLESTATS.ordinal(), new TableStatsTableDescriptor(this));
    schemaInfoTableDescriptors.set(DEFINED_TABLES.PARTITIONS.ordinal(), new PartitionsTableDescriptor(this));
    schemaInfoTableDescriptors.set(DEFINED_TABLES.PARTITION_KEYS.ordinal(), new PartitionKeysTableDescriptor(this));
    schemaInfoTableDescriptors.set(DEFINED_TABLES.CLUSTER.ordinal(), new ClusterTableDescriptor(this));
  }

  public boolean isSystemDatabase(String databaseName) {
    boolean result = false;
    
    if (databaseName != null && !databaseName.isEmpty()) {
      result = DATABASE_NAME.equalsIgnoreCase(databaseName);
    }
    
    return result;
  }
  
  public String getSystemDatabaseName() {
    return DATABASE_NAME;
  }
  
  public List<String> getAllSystemTables() {
    List<String> systemTableNames = TUtil.newList();
    
    for (TableDescriptor descriptor: schemaInfoTableDescriptors) {
      systemTableNames.add(descriptor.getTableNameString());
    }
    
    return systemTableNames;
  }
  
  private TableDescriptor getTableDescriptor(String tableName) {
    TableDescriptor tableDescriptor = null;
    
    if (tableName == null || tableName.isEmpty()) {
      throw new NoSuchTableException(tableName);
    }
    
    tableName = tableName.toUpperCase();
    for (int idx = 0; idx < schemaInfoTableDescriptors.size(); idx++) {
      TableDescriptor testDescriptor = schemaInfoTableDescriptors.get(idx);
      if (testDescriptor.getTableNameString().equalsIgnoreCase(tableName)) {
        tableDescriptor = testDescriptor;
        break;
      }
    }
    
    return tableDescriptor;
  }
  
  public CatalogProtos.TableDescProto getTableDesc(String tableName) {
    TableDescriptor tableDescriptor;
    
    tableDescriptor = getTableDescriptor(tableName);
    if (tableDescriptor == null) {
      throw new NoSuchTableException(DATABASE_NAME, tableName);
    }
    
    return tableDescriptor.getTableDescription();
  }
  
  public boolean existTable(String tableName) {
    return getTableDescriptor(tableName) != null;
  }
  
  protected String getTablePath() {
    return StoreType.SYSTEM.name().toUpperCase();
  }
}
