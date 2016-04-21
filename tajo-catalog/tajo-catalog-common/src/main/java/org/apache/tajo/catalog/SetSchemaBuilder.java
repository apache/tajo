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
import org.apache.commons.collections.collection.UnmodifiableCollection;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.schema.QualifiedIdentifier;
import org.apache.tajo.schema.Schema.NamedPrimitiveType;
import org.apache.tajo.schema.Schema.NamedStructType;
import org.apache.tajo.schema.Schema.NamedType;
import org.apache.tajo.type.Type;

import java.util.*;

import static org.apache.tajo.catalog.FieldConverter.toQualifiedIdentifier;
import static org.apache.tajo.schema.IdentifierPolicy.DefaultPolicy;

public class SetSchemaBuilder implements SchemaBuilder.SchemaCollector {
  private final Set<QualifiedIdentifier> nameSet = new HashSet<>();
  private final ImmutableList.Builder<NamedType> fields = new ImmutableList.Builder();

  @Override
  public void add(NamedType namedType) {
    if (!nameSet.contains(namedType.name())) {
      fields.add(namedType);
      nameSet.add(namedType.name());
    }
  }

  @Override
  public void addAll(Iterator<NamedType> fields) {
    while (fields.hasNext()) {
      add(fields.next());
    }
  }

  @Override
  public void addAll(Iterable<NamedType> fields) {
    for (NamedType n : fields) {
      add(n);
    }
  }

  @Override
  public ImmutableCollection<NamedType> build() {
    return fields.build();
  }
}
