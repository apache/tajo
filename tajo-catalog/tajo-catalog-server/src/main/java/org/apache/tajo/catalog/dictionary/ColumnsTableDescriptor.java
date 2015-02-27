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

class ColumnsTableDescriptor extends AbstractTableDescriptor {
  
  private static final String TABLENAME = "columns";
  private final ColumnDescriptor[] columns = new ColumnDescriptor[] {
      new ColumnDescriptor("tid", Type.INT4, 0), // just key for DBMS
      new ColumnDescriptor("column_name", Type.TEXT, 0), // column name
      new ColumnDescriptor("ordinal_position", Type.INT4, 0), // the ordinal position in a schema
      new ColumnDescriptor("nested_field_num", Type.INT4, 0), // the number of child nested fields
      new ColumnDescriptor("data_type", Type.TEXT, 0),
      new ColumnDescriptor("type_length", Type.INT4, 0)
  };
  
  public ColumnsTableDescriptor(InfoSchemaMetadataDictionary metadataDictionary) {
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
