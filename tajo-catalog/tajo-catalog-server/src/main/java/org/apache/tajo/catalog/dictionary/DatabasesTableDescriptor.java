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

import org.apache.tajo.common.TajoDataTypes.Type;

class DatabasesTableDescriptor extends AbstractTableDescriptor {
  
  private static final String TABLENAME = "DATABASES_";
  
  private final ColumnDescriptor[] columns = new ColumnDescriptor[] {
      new ColumnDescriptor("DB_ID", Type.INT4, 0),
      new ColumnDescriptor("DB_NAME", Type.TEXT, 0),
      new ColumnDescriptor("SPACE_ID", Type.INT4, 0)
  };
  
  public DatabasesTableDescriptor(SystemMetadataDictionary metadataDictionary) {
    super(metadataDictionary);
  }

  @Override
  public String getTableNameString() {
    return TABLENAME;
  }

  @Override
  protected ColumnDescriptor[] getColumnDescriptors() {
    return columns;
  }

}
