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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import org.apache.tajo.exception.NotImplementedException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.schema.Field;
import org.apache.tajo.schema.Identifier;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.schema.QualifiedIdentifier;

import javax.annotation.Nullable;
import java.util.Collection;

public class FieldConverter {

  public static QualifiedIdentifier toQualifiedIdentifier(String name) {
    final Collection<String> elems = ImmutableList.copyOf(name.split("\\."));
    final Collection<Identifier> identifiers = Collections2.transform(elems, new Function<String, Identifier>() {
      @Override
      public Identifier apply(@Nullable String input) {
        return Identifier._(input, IdentifierUtil.isShouldBeQuoted(input));
      }
    });
    return QualifiedIdentifier.$(identifiers);
  }

  public static Field convert(Column column) {
    if (column.type.isStruct() && column.getTypeDesc().getNestedSchema() == null) {
      throw new TajoRuntimeException(new NotImplementedException("record type projection"));
    }
    return new Field(toQualifiedIdentifier(column.getQualifiedName()), column.type);
  }

  public static Column convert(Field field) {
    return new Column(field.name().interned(), field.type());
  }
}
