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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * Utility methods for nested field
 */
public class NestedPathUtil {
  public static final String PATH_DELIMITER = "/";

  public static final List<String> ROOT_PATH = Collections.unmodifiableList(new ArrayList<String>());

  public static boolean isPath(String name) {
    return name.indexOf(PATH_DELIMITER.charAt(0)) >= 0;
  }

  public static String makePath(String[] parts) {
    return makePath(parts, 0);
  }

  public static String makePath(String[] parts, int startIndex) {
    return makePath(parts, startIndex, parts.length);
  }

  /**
   * Make a nested field path
   *
   * @param parts path parts
   * @param startIndex startIndex
   * @param depth Depth
   * @return Path
   */
  public static String makePath(String[] parts, int startIndex, int depth) {
    Preconditions.checkArgument(startIndex <= (parts.length - 1));

    StringBuilder sb = new StringBuilder();
    for (int i = startIndex; i < depth; i++) {
      sb.append(PATH_DELIMITER);
      sb.append(parts[i].toString());
    }

    return sb.toString();
  }

  public static Column lookupPath(Column nestedField, String [] paths) {
    // We assume that path starts with '/', causing an empty string "" at 0 in the path splits.
    // So, we should start the index from 1 instead of 0.
    return lookupColumnInternal(nestedField, paths, 1);
  }

  private static Column lookupColumnInternal(Column currentColumn, String [] paths, int depth) {
    Column found = null;

    if (currentColumn.getDataType().getType() == Type.RECORD) {
      found = currentColumn.typeDesc.nestedRecordSchema.getColumn(paths[depth]);
    }

    if (found != null) {
      if (found.getDataType().getType() == Type.RECORD) {
        return lookupColumnInternal(found, paths, depth + 1);
      } else {
        return found;
      }
    } else {
      throw new NoSuchFieldError(makePath(paths));
    }
  }

  public static String [] convertColumnsToPaths(Column [] columns) {
    String [] paths = new String[columns.length];

    for (int i = 0; i < columns.length; i++) {
      paths[i] = columns[i].getSimpleName();
    }

    return paths;
  }

  public static ImmutableMap<String, Type> buildTypeMap(Iterable<Column> schema, String [] targetPaths) {

    ImmutableMap.Builder<String, Type> builder = ImmutableMap.builder();
    for (Column column : schema) {

      // Keep types which only belong to projected paths
      // For example, assume that a projected path is 'name/first_name', where name is RECORD and first_name is TEXT.
      // In this case, we should keep two types:
      // * name - RECORD
      // * name/first_name TEXT
      for (String p : targetPaths) {
        if (p.startsWith(column.getSimpleName())) {
          builder.put(column.getSimpleName(), column.getDataType().getType());
        }
      }
    }

    return builder.build();
  }
}
