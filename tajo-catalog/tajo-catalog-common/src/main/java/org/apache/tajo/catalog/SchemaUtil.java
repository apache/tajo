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

package org.apache.tajo.catalog;

import org.apache.tajo.util.TUtil;

import java.util.List;

import static org.apache.tajo.common.TajoDataTypes.DataType;
import static org.apache.tajo.common.TajoDataTypes.Type;

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
    for(Column col : left.getRootColumns()) {
      if (!merged.containsByQualifiedName(col.getQualifiedName())) {
        merged.addColumn(col);
      }
    }
    for(Column col : right.getRootColumns()) {
      if (merged.containsByQualifiedName(col.getQualifiedName())) {
        merged.addColumn("?fake" + (tmpColumnSeq++), col.getDataType());
      } else {
        merged.addColumn(col);
      }
    }

    // if overflow
    if (tmpColumnSeq < 0) {
      tmpColumnSeq = 0;
    }
    return merged;
  }

  /**
   * Get common columns to be used as join keys of natural joins.
   */
  public static Schema getNaturalJoinColumns(Schema left, Schema right) {
    Schema common = new Schema();
    for (Column outer : left.getRootColumns()) {
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

  public static DataType [] toDataTypes(Schema schema) {
    DataType[] types = new DataType[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      types[i] = schema.getColumn(i).getDataType();
    }
    return types;
  }

  public static Type [] toTypes(Schema schema) {
    Type [] types = new Type[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      types[i] = schema.getColumn(i).getDataType().getType();
    }
    return types;
  }

  public static String [] toSimpleNames(Schema schema) {
    String [] names = new String[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      names[i] = schema.getColumn(i).getSimpleName();
    }
    return names;
  }

  /**
   * Column visitor interface
   */
  public static interface ColumnVisitor {
    public void visit(int depth, List<String> path, Column column);
  }

  /**
   * It allows a column visitor to traverse all columns in a schema in a depth-first order.
   * @param schema
   * @param function
   */
  public static void visitSchema(Schema schema, ColumnVisitor function) {
      for(Column col : schema.getRootColumns()) {
        visitInDepthFirstOrder(0, NestedPathUtil.ROOT_PATH, function, col);
      }
  }

  /**
   * A recursive function to traverse all columns in a schema in a depth-first order.
   *
   * @param depth Nested depth. 0 is root column.
   * @param function Visitor
   * @param column Current visiting column
   */
  private static void visitInDepthFirstOrder(int depth,
                                             final List<String> path,
                                             ColumnVisitor function,
                                             Column column) {

    if (column.getDataType().getType() == Type.RECORD) {
      for (Column nestedColumn : column.typeDesc.nestedRecordSchema.getRootColumns()) {
        List<String> newPath = TUtil.newList(path);
        newPath.add(column.getQualifiedName());

        visitInDepthFirstOrder(depth + 1, newPath, function, nestedColumn);
      }
      function.visit(depth, path, column);
    } else {
      function.visit(depth, path, column);
    }
  }

  public static String toDisplayString(Schema schema) {
    StringBuilder sb = new StringBuilder();
    DDLBuilder.buildSchema(sb, schema);
    return sb.toString();
  }
}
