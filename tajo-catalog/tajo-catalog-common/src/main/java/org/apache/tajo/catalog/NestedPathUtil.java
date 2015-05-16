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
import org.apache.tajo.common.TajoDataTypes.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

  /**
   * Lookup the actual column corresponding to a given path.
   * We assume that a path starts with the slash '/' and it
   * does not include the root field.
   *
   * @param nestedField Nested column
   * @param path Path which starts with '/';
   * @return Column corresponding to the path
   */
  public static Column lookupPath(Column nestedField, String path) {
    Preconditions.checkArgument(path.charAt(0) == PATH_DELIMITER.charAt(0),
        "A nested field path must start with slash '/'.");

    // We assume that path starts with '/', causing an empty string "" at 0 in the path splits.
    // So, we should start the index from 1 instead of 0.
    return lookupPath(nestedField, path.split(PATH_DELIMITER));
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
}
