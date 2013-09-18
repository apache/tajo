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

package org.apache.tajo.engine.utils;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.DataType;

public class SchemaUtil {
  public static Schema merge(Schema left, Schema right) {
    Schema merged = new Schema();
    for(Column col : left.getColumns()) {
      if (!merged.contains(col.getQualifiedName())) {
        merged.addColumn(col);
      }
    }
    for(Column col : right.getColumns()) {
      if (!merged.contains(col.getQualifiedName())) {
        merged.addColumn(col);
      }
    }
    
    return merged;
  }
  
  public static Schema getCommons(Schema left, Schema right) {
    Schema common = new Schema();
    for (Column outer : left.getColumns()) {
      for (Column inner : right.getColumns()) {
        if (outer.getColumnName().equals(inner.getColumnName()) &&
            outer.getDataType().equals(inner.getDataType())) {
          common.addColumn(outer.getColumnName(), outer.getDataType());
        }
      }
    }
    
    return common;
  }

  public static DataType[] newNoNameSchema(DataType... types) {
    DataType [] dataTypes = types.clone();
    return dataTypes;
  }
}
