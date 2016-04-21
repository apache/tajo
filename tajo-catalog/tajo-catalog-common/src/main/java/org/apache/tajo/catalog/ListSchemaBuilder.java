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

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import org.apache.tajo.schema.Schema.NamedType;

import java.util.Iterator;

public class ListSchemaBuilder implements SchemaBuilder.SchemaCollector {
  private final ImmutableList.Builder<NamedType> fields = new ImmutableList.Builder();

  @Override
  public void add(NamedType namedType) {
    fields.add(namedType);
  }

  @Override
  public void addAll(Iterator<NamedType> fields) {
    this.fields.addAll(fields);
  }

  @Override
  public void addAll(Iterable<NamedType> fields) {
    this.fields.addAll(fields);
  }

  @Override
  public ImmutableCollection<NamedType> build() {
    return fields.build();
  }
}
