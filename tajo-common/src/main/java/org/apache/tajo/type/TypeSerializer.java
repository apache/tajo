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
import com.google.common.collect.ImmutableList;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.schema.Field;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.schema.QualifiedIdentifier;
import org.apache.tajo.util.StringUtils;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Stack;

import static org.apache.tajo.schema.IdentifierPolicy.DefaultPolicy;
import static org.apache.tajo.type.Type.Char;
import static org.apache.tajo.type.Type.Varchar;

/**
 * This class enables to serialize a type into a string representation and vice versa.
 */
public class TypeSerializer {

  /**
   * Serialize a type into a string representation
   * @param type A type
   * @return A type string representation
   */
  public static String serialize(Type type) {
    StringBuilder sb = new StringBuilder(type.baseType().name());

    if (type.isTypeParameterized()) {
      sb.append("<");
      sb.append(StringUtils.join(type.getTypeParameters(), ",", new Function<Type, String>() {
        @Override
        public String apply(@Nullable Type type) {
          return TypeSerializer.serialize(type);
        }
      }));
      sb.append(">");
    }

    // Assume all parameter values are integers.
    if (type.isValueParameterized()) {
      sb.append("(");
      sb.append(StringUtils.join(type.getValueParameters(), ","));
      sb.append(")");
    }

    if (type.isStruct()) {
      Record record = (Record) type;
      sb.append("[");
      sb.append(StringUtils.join(record.fields(), ",", new Function<Field, String>() {
        @Override
        public String apply(@Nullable Field field) {
          return serializeField(field);
        }
      }));
      sb.append("]");
    }

    return sb.toString();
  }

  /**
   * Make a string from a field
   * @param field A field
   * @return String representation for a field
   */
  static String serializeField(Field field) {
    return field.name().raw(DefaultPolicy()) + " " + serialize(field.type());
  }

  /**
   * Parse and transform a type string representation to a Type.
   * @param signature Type string representation
   * @return Type
   */
  public static Type deserialize(String signature) {

    // termination condition in this recursion
    if (!(signature.contains("<") || signature.contains("(") || signature.contains("["))) {
      return createType(signature,
          ImmutableList.<Type>of(),
          ImmutableList.<Integer>of(),
          ImmutableList.<Field>of());
    }

    final Stack<Character> stack = new Stack<>();
    final Stack<Integer> spanStack = new Stack<>();
    String baseType = null;
    for (int i = 0; i < signature.length(); i++) {
      char c = signature.charAt(i);

      if (c == '<') {
        if (stack.isEmpty()) {
          assertArgument(baseType == null, "Expected baseName to be null");
          baseType = signature.substring(0, i);
        }
        stack.push('<');
        spanStack.push(i + 1);

      } else if (c == '>') {
        assertArgument(stack.pop() == '<', "Bad signature: '%s'", signature);
        int paramStartIdx = spanStack.pop();

        if (stack.isEmpty()) { // ensure outermost parameters
          return createType(baseType,
              parseList(signature.substring(paramStartIdx, i), new Function<String, Type>() {
                @Override
                public Type apply(@Nullable String s) {
                  return deserialize(s);
                }
              }),
              ImmutableList.<Integer>of(),
              ImmutableList.<Field>of());
        }

      } else if (c == '[') {
        if (stack.isEmpty()) {
          assertArgument(baseType == null, "Expected baseName to be null");
          baseType = signature.substring(0, i);
        }

        stack.push('[');
        spanStack.push(i + 1);

      } else if (c == ']') {
        assertArgument(stack.pop() == '[', "Bad signature: '%s'", signature);

        int paramStartIdx = spanStack.pop();
        if (stack.isEmpty()) { // ensure outermost parameters
          return createType(baseType,
              ImmutableList.<Type>of(),
              ImmutableList.<Integer>of(),
              parseList(signature.substring(paramStartIdx, i), new Function<String, Field>() {
                @Override
                public Field apply(@Nullable String s) {
                  return parseField(s);
                }
              }));
        }

      } else if (c == '(') {
        if (stack.isEmpty()) {
          assertArgument(baseType == null, "Expected baseName to be null");
          baseType = signature.substring(0, i);
        }
        stack.push('(');
        spanStack.push(i + 1);

      } else if (c == ')') {
        assertArgument(stack.pop() == '(', "Bad signature: '%s'", signature);
        int paramStartIdx = spanStack.pop();

        if (stack.isEmpty()) { // ensure outermost parameters
          return createType(baseType,
              ImmutableList.<Type>of(),
              parseList(signature.substring(paramStartIdx, i), new Function<String, Integer>() {
                @Override
                public Integer apply(@Nullable String s) {
                  return parseValue(s);
                }
              }),
              ImmutableList.<Field>of());
        }
      }
    }

    return null;
  }

  public static int parseValue(String literal) {
    try {
      return Integer.parseInt(literal);
    } catch (NumberFormatException e) {
      throw new TajoInternalError(e);
    }
  }

  /**
   * Parse a string delimited by comma into a list of object instances depending on <pre>itemParser</pre>.
   * @param str String delimited by comma
   * @param itemParser A function to transform a string to an object.
   * @param <T> Type to be transformed from a string
   * @return List of object instances
   */
  static <T> List<T> parseList(String str, Function<String, T> itemParser) {
    if (!str.contains(",")) { // if just one item
      return ImmutableList.of(itemParser.apply(str));
    }

    final ImmutableList.Builder<T> fields = ImmutableList.builder();
    final Stack<Character> stack = new Stack<>();
    int paramStartIdx = 0;
    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);

      if (c == '<' || c == '[' || c == '(') {
        stack.push(c);
      } else if (c == '>') {
        assertArgument(stack.pop() == '<', "Bad signature: '%s'", str);
      } else if (c == ']') {
        assertArgument(stack.pop() == '[', "Bad signature: '%s'", str);
      } else if (c == ')') {
        assertArgument(stack.pop() == '(', "Bad signature: '%s'", str);
      } else if (c == ',') {
        if (stack.isEmpty()) { // ensure outermost type parameters
          fields.add(itemParser.apply(str.substring(paramStartIdx, i)));
          paramStartIdx = i + 1;
        }
      }
    }

    assertArgument(stack.empty(), "Bad signature: '%s'", str);
    if (paramStartIdx < str.length()) {
      fields.add(itemParser.apply(str.substring(paramStartIdx, str.length())));
    }

    return fields.build();
  }

  /**
   * Make a field from a string representation
   * @param str String
   * @return Field
   */
  static Field parseField(String str) {
    // A field consists of an identifier and a type, and they are delimited by space.
    if (!str.contains(" ")) {
      assertArgument(false, "Bad field signature: '%s'", str);
    }

    // Stack to track the nested bracket depth
    Stack<Character> stack = new Stack<>();
    int paramStartIdx = 0;
    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);

      if (c == '<' || c == '[' || c == '(') {
        stack.push(c);
      } else if (c == '>') { // for validation
        assertArgument(stack.pop() == '<', "Bad field signature: '%s'", str);
      } else if (c == ']') { // for validation
        assertArgument(stack.pop() == '[', "Bad field signature: '%s'", str);
      } else if (c == ')') { // for validation
        assertArgument(stack.pop() == '(', "Bad field signature: '%s'", str);

      } else if (c == ' ') {
        if (stack.isEmpty()) { // ensure outermost type parameters
          QualifiedIdentifier identifier =
              IdentifierUtil.makeIdentifier(str.substring(paramStartIdx, i), DefaultPolicy());
          String typePart = str.substring(i + 1, str.length());
          return new Field(identifier, deserialize(typePart));
        }
      }
    }

    return null;
  }

  static Type createType(String baseTypeStr,
                                List<Type> typeParams,
                                List<Integer> valueParams,
                                List<Field> fieldParams) {
    final TajoDataTypes.Type baseType;

    try {
      baseType = TajoDataTypes.Type.valueOf(baseTypeStr);
    } catch (Throwable t) {
      throw new TajoInternalError(new UnsupportedException(baseTypeStr));
    }

    switch (baseType) {
    case CHAR: {
      assertArgument(valueParams.size() == 1,
          "Char type requires 1 integer parameters, but it takes (%s).", StringUtils.join(typeParams));
      return Char(valueParams.get(0));
    }
    case VARCHAR: {
      assertArgument(valueParams.size() == 1,
          "Varchar type requires 1 integer parameters, but it takes (%s).", StringUtils.join(typeParams));
      return Varchar(valueParams.get(0));
    }
    case TEXT: return Type.Text;

    case BOOLEAN: return Type.Bool;
    case INT1: return Type.Int1;
    case INT2: return Type.Int2;
    case INT4: return Type.Int4;
    case INT8: return Type.Int8;
    case FLOAT4: return Type.Float4;
    case FLOAT8: return Type.Float8;
    case NUMERIC: {
      if (valueParams.size() == 0) {
        return Numeric.Numeric();

      } else {
        for (Object p : valueParams) {
          assertArgument(p instanceof Integer, "Numeric type requires integer parameters");
        }
        if (valueParams.size() == 1) {
          return Numeric.Numeric(valueParams.get(0));
        } else if (valueParams.size() == 2) {
          return Numeric.Numeric(valueParams.get(0), valueParams.get(1));
        } else {
          assertArgument(false,
              "Numeric type can take 2 or less integer parameters, but it takes (%s).", StringUtils.join(valueParams));
        }
      }
    }

    case DATE: return Type.Date;
    case TIME: return Type.Time;
    case TIMESTAMP: return Type.Timestamp;
    case INTERVAL: return Type.Interval;
    case BLOB: return Type.Blob;
    case INET4: return Type.Inet4;

    case ARRAY: {
      assertArgument(typeParams.size() == 1,
          "Array Type requires 1 type parameters, but it takes (%s).", StringUtils.join(typeParams));
      return Type.Array(typeParams.get(0));
    }
    case MAP: {
      assertArgument(typeParams.size() == 2,
          "Map Type requires 2 type parameters, but it takes (%s).", StringUtils.join(typeParams));

      return Type.Map(typeParams.get(0), typeParams.get(1));
    }
    case RECORD: {
      assertArgument(fieldParams.size() >= 1,
          "Record Type requires at least 1 field parameters, but it takes (%s).", StringUtils.join(fieldParams));
      return Type.Record(fieldParams);
    }

    default:
      throw new TajoInternalError(new UnsupportedException(baseTypeStr));
    }
  }

  static void assertArgument(boolean condition, String message, Object ... arguments) {
    if (!condition) {
      throw new TajoInternalError(String.format(message, arguments));
    }
  }
}
