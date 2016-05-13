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

package org.apache.tajo.schema;

import com.google.common.collect.ImmutableList;
import org.apache.tajo.util.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * A field is a pair of a name and a type. Schema is an ordered list of fields.
 */
public class Schema implements Iterable<Field> {
  private final ImmutableList<Field> fields;

  public Schema(Collection<Field> fields) {
    this.fields = ImmutableList.copyOf(fields);
  }

  public static Schema Schema(Field...fields) {
    return new Schema(Arrays.asList(fields));
  }

  public static Schema Schema(Collection<Field> fields) {
    return new Schema(fields);
  }

  @Override
  public String toString() {
    return StringUtils.join(fields, ", ");
  }

  @Override
  public Iterator<Field> iterator() {
    return fields.iterator();
  }
}
