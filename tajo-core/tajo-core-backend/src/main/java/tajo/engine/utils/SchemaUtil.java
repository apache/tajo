/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.engine.utils;

import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.engine.parser.QueryBlock;

import java.util.Collection;

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

  public static Schema merge(QueryBlock.FromTable [] fromTables) {
    Schema merged = new Schema();
    for (QueryBlock.FromTable table : fromTables) {
      merged.addColumns(table.getSchema());
    }

    return merged;
  }
  
  public static Schema getCommons(Schema left, Schema right) {
    Schema common = new Schema();
    for (Column outer : left.getColumns()) {
      for (Column inner : right.getColumns()) {
        if (outer.getColumnName().equals(inner.getColumnName()) &&
            outer.getDataType() == inner.getDataType()) {
          common.addColumn(outer.getColumnName(), outer.getDataType());
        }
      }
    }
    
    return common;
  }

  public static Schema mergeAllWithNoDup(Collection<Column>...columnList) {
    Schema merged = new Schema();
    for (Collection<Column> columns : columnList) {
      for (Column col : columns) {
        if (merged.contains(col.getQualifiedName())) {
          continue;
        }
        merged.addColumn(col);
      }
    }

    return merged;
  }

  public static Schema getProjectedSchema(Schema inSchema, Collection<Column> columns) {
    Schema projected = new Schema();
    for (Column col : columns) {
      if (inSchema.contains(col.getQualifiedName())) {
        projected.addColumn(col);
      }
    }

    return projected;
  }

  public static DataType[] newNoNameSchema(DataType... types) {
    DataType [] dataTypes = types.clone();
    return dataTypes;
  }
}
