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

package org.apache.tajo.type;

import org.apache.tajo.schema.Field;

public abstract class TypeVisitor {
  public void visit(Type type) {
    switch (type.kind) {
    case ARRAY:
      visitArray((Array) type);
      break;
    case RECORD:
      visitRecord((Record) type);
      break;
    case MAP:
      visitMap((Map) type);
      break;
    default:
      visitPrimitive(type);
    }
  }

  void visitPrimitive(Type type) {}

  void visitMap(Map map) {
    visit(map.keyType());
    visit(map.valueType());
  }
  void visitArray(Array array) {
    visit(array.elementType());
  }
  void visitRecord(Record record) {
    for (Field field : record.fields()) {
      visit(field.type());
    }
  }
}
