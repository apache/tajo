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
import org.apache.tajo.catalog.TableDesc;

public class SchemaUtil {
  // See TAJO-914 bug.
  //
  // Its essential problem is that constant value is evaluated multiple times at each scan.
  // As a result, join nodes can take the child nodes which have the same named fields.
  // Because current schema does not allow the same name and ignore the duplicated schema,
  // it finally causes the in-out schema mismatch between the parent and child nodes.
  //
  // tmpColumnSeq is a hack to avoid the above problem by keeping duplicated constant values as different name fields.
  // The essential solution would be https://issues.apache.org/jira/browse/TAJO-895.
  static int tmpColumnSeq = 0;
  public static Schema merge(Schema left, Schema right) {
    Schema merged = new Schema();
    for(Column col : left.getColumns()) {
      if (!merged.containsByQualifiedName(col.getQualifiedName())) {
        merged.addColumn(col);
      }
    }
    for(Column col : right.getColumns()) {
      if (merged.containsByQualifiedName(col.getQualifiedName())) {
        merged.addColumn("?fake" + (tmpColumnSeq++), col.getDataType());
      } else {
        merged.addColumn(col);
      }
    }

    if (tmpColumnSeq > Integer.MAX_VALUE) {
      tmpColumnSeq = 0;
    }
    return merged;
  }

  /**
   * Get common columns to be used as join keys of natural joins.
   */
  public static Schema getNaturalJoinColumns(Schema left, Schema right) {
    Schema common = new Schema();
    for (Column outer : left.getColumns()) {
      if (!common.containsByName(outer.getSimpleName()) && right.containsByName(outer.getSimpleName())) {
        common.addColumn(new Column(outer.getSimpleName(), outer.getDataType()));
      }
    }
    
    return common;
  }

  public static Schema getQualifiedLogicalSchema(TableDesc tableDesc, String tableName) {
    Schema logicalSchema = new Schema(tableDesc.getLogicalSchema());
    if (tableName != null) {
      logicalSchema.setQualifier(tableName);
    }
    return logicalSchema;
  }

  public static <T extends Schema> T clone(Schema schema) {
    try {
      T copy = (T) schema.clone();
      return copy;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }
}
