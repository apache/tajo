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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.QualifiedIdentifierProto;
import org.apache.tajo.common.TajoDataTypes.TypeElement;
import org.apache.tajo.common.TajoDataTypes.TypeProto;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.schema.Field;
import org.apache.tajo.schema.QualifiedIdentifier;

import javax.annotation.Nullable;
import java.util.*;

import static java.util.Collections.EMPTY_LIST;
import static org.apache.tajo.Assert.assertCondition;
import static org.apache.tajo.common.TajoDataTypes.Type.*;
import static org.apache.tajo.type.Type.Array;
import static org.apache.tajo.type.Type.Map;
import static org.apache.tajo.type.Type.Record;

public class TypeProtobufEncoder {
  public static TypeProto encode(Type type) {

    final TypeProto.Builder builder = TypeProto.newBuilder();
    new Visitor(builder).visit(type);
    return builder.build();
  }

  public static Type decode(TypeProto proto) {
    Stack<Type> stack = new Stack<>();

    for (int curIdx = 0; curIdx < proto.getElementsCount(); curIdx++) {
      TypeElement e = proto.getElements(curIdx);

      if (e.hasChildNum()) { // if it is a type-parameterized, that is
        List<Type> childTypes = popMultiItems(stack, e.getChildNum());

        if (e.getKind() == ARRAY || e.getKind() == MAP) {
          stack.push(createTypeParameterizedType(e, childTypes));

        } else { // record
          assertCondition(e.getKind() == RECORD,
              "This type must be RECORD type.");
          assertCondition(childTypes.size() == e.getFieldNamesCount(),
              "The number of Field types and names must be equal.");

          ImmutableList.Builder<Field> fields = ImmutableList.builder();
          for (int i = 0; i < childTypes.size(); i++) {
            fields.add(new Field(QualifiedIdentifier.fromProto(e.getFieldNames(i)), childTypes.get(i)));
          }
          stack.push(Record(fields.build()));
        }

      } else {
        stack.push(createPrimitiveType(e));
      }
    }

    assertCondition(stack.size() == 1, "Stack size has two or more items.");
    return stack.pop();
  }

  static List<Type> popMultiItems(Stack<Type> stack, int num) {
    List<Type> typeParams = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      typeParams.add(0, stack.pop());
    }
    return typeParams;
  }

  static boolean isValueParameterized(TajoDataTypes.Type baseType) {
    return baseType == CHAR || baseType == VARCHAR || baseType == NUMERIC;
  }

  static Type createPrimitiveType(TypeElement element) {
    assertPrimitiveType(element);

    if (isValueParameterized(element.getKind())) {
      return TypeFactory.create(element.getKind(), EMPTY_LIST, element.getValueParamsList(), EMPTY_LIST);
    } else if (element.getKind() == PROTOBUF) { // TODO - PROTOBUF type should be removed later
      return new Protobuf(element.getStringParams(0));
    } else {
      return TypeFactory.create(element.getKind());
    }
  }

  static Type createTypeParameterizedType(TypeElement element, List<Type> childTypes) {
    switch (element.getKind()) {
    case ARRAY:
      return Array(childTypes.get(0));
    case MAP:
      return Map(childTypes.get(0), childTypes.get(1));
    default:
      throw new TajoInternalError(element.getKind().name() + " is not a type-parameterized type.");
    }
  }

  static void assertPrimitiveType(TypeElement element) {
    TajoDataTypes.Type baseType = element.getKind();
    if (baseType == MAP || baseType == RECORD || baseType == ARRAY) {
      throw new TajoInternalError(baseType.name() + " is not a primitive type.");
    }
  }

  static class Visitor extends TypeVisitor {
    final TypeProto.Builder builder;

    Visitor(TypeProto.Builder builder) {
      this.builder = builder;
    }

    @Override
    public void visitPrimitive(Type type) {
      TypeElement.Builder typeElemBuilder = TypeElement.newBuilder()
          .setKind(type.kind);

      if (type.isValueParameterized()) {
        typeElemBuilder.addAllValueParams(type.getValueParameters());
      } else if (type.kind == PROTOBUF) {
        typeElemBuilder.addStringParams(((Protobuf)type).getMessageName());
      }

      builder.addElements(typeElemBuilder);
    }

    @Override
    public void visitMap(Map map) {
      super.visitMap(map);
      builder.addElements(TypeElement.newBuilder()
             .setKind(map.kind)
             .setChildNum(2)
          );
    }

    @Override
    public void visitArray(Array array) {
      super.visitArray(array);
      builder
          .addElements(TypeElement.newBuilder()
              .setKind(array.kind)
              .setChildNum(1)
          );
    }

    @Override
    public void visitRecord(Record record) {
      super.visitRecord(record);

      Collection<QualifiedIdentifierProto> field_names =
          Collections2.transform(record.fields(), new Function<Field, QualifiedIdentifierProto>() {
        @Override
        public QualifiedIdentifierProto apply(@Nullable Field field) {
          return field.name().getProto();
        }
      });

      builder
          .addElements(TypeElement.newBuilder()
              .setChildNum(record.size())
              .addAllFieldNames(field_names)
              .setKind(RECORD));
    }
  }
}
