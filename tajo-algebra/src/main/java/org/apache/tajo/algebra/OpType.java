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

package org.apache.tajo.algebra;

import com.google.gson.*;

import java.lang.reflect.Type;

public enum OpType {

  // relational operators
  Projection(Projection.class),
  Limit(Limit.class),
  Sort(Sort.class),
  Having(Having.class),
  Aggregation(Aggregation.class),
  Join(Join.class),
  Filter(Selection.class),
  Union(SetOperation.class),
  Except(SetOperation.class),
  Intersect(SetOperation.class),
  SimpleTableSubQuery(SimpleTableSubQuery.class),
  TablePrimaryTableSubQuery(TablePrimarySubQuery.class),
  RelationList(RelationList.class),
  Relation(Relation.class),
  ScalarSubQuery(ScalarSubQuery.class),
  Explain(Explain.class),
  Window(Window.class),

  // Data definition language
  CreateDatabase(CreateDatabase.class),
  DropDatabase(DropDatabase.class),
  CreateTable(CreateTable.class),
  DropTable(DropTable.class),
  AlterTablespace(AlterTablespace.class),
  AlterTable(AlterTable.class),
  TruncateTable(TruncateTable.class),

  // Insert or Update
  Insert(Insert.class),

  // Logical Operators
  And(BinaryOperator.class),
  Or(BinaryOperator.class),
  Not(NotExpr.class),

  // Comparison Predicates
  Equals(BinaryOperator.class),
  NotEquals(BinaryOperator.class),
  LessThan(BinaryOperator.class),
  LessThanOrEquals(BinaryOperator.class),
  GreaterThan(BinaryOperator.class),
  GreaterThanOrEquals(BinaryOperator.class),

  // Other predicates
  Between(BetweenPredicate.class),
  CaseWhen(CaseWhenPredicate.class),
  IsNullPredicate(IsNullPredicate.class),
  InPredicate(InPredicate.class),
  ValueList(ValueListExpr.class),
  ExistsPredicate(ExistsPredicate.class),

  // String Operator or Pattern Matching Predicates
  LikePredicate(PatternMatchPredicate.class),
  SimilarToPredicate(PatternMatchPredicate.class),
  Regexp(PatternMatchPredicate.class),
  Concatenate(BinaryOperator.class),

  // Arithmetic Operators
  Plus(BinaryOperator.class),
  Minus(BinaryOperator.class),
  Multiply(BinaryOperator.class),
  Divide(BinaryOperator.class),
  Modular(BinaryOperator.class),

  // Other Expressions
  Sign(SignedExpr.class),
  Column(ColumnReferenceExpr.class),
  Target(NamedExpr.class),
  Function(FunctionExpr.class),
  Asterisk(QualifiedAsteriskExpr.class),

  // Set Functions
  WindowFunction(WindowFunctionExpr.class),
  CountRowsFunction(CountRowsFunctionExpr.class),
  GeneralSetFunction(GeneralSetFunctionExpr.class),

  // Literal
  DataType(DataTypeExpr.class),
  Cast(CastExpr.class),
  Literal(LiteralValue.class),
  NullLiteral(NullLiteral.class),
  TimeLiteral(TimeLiteral.class),
  DateLiteral(DateLiteral.class),
  TimestampLiteral(TimestampLiteral.class),
  IntervalLiteral(IntervalLiteral.class);

  private Class baseClass;

  OpType() {
    this.baseClass = Expr.class;
  }
  OpType(Class clazz) {
    this.baseClass = clazz;
  }

  public Class getBaseClass() {
    return this.baseClass;
  }

  public static class JsonSerDer implements JsonSerializer<OpType>,
                                            JsonDeserializer<OpType> {

    @Override
    public JsonElement serialize(OpType src, Type typeOfSrc,
                                 JsonSerializationContext context) {
      return new JsonPrimitive(src.name());
    }

    @Override
    public OpType deserialize(JsonElement json, Type typeOfT,
                                      JsonDeserializationContext context)
        throws JsonParseException {
      return OpType.valueOf(json.getAsString());
    }
  }

  public static boolean isLogicalType(OpType type) {
    return type == And || type == Or;
  }

  public static boolean isComparisonType(OpType type) {
    return
        type == OpType.Equals ||
        type == OpType.NotEquals ||
        type == OpType.LessThan ||
        type == OpType.GreaterThan ||
        type == OpType.LessThanOrEquals ||
        type == OpType.GreaterThanOrEquals;
  }

  public static boolean isArithmeticType(OpType type) {
    return
        type == Plus ||
        type == Minus ||
        type == Multiply ||
        type == Divide ||
        type == Modular;
  }

  /**
   * Check if it is one of the literal types.
   *
   * @param type The type to be checked
   * @return True if it is one of the literal types. Otherwise, it returns False.
   */
  public static boolean isLiteralType(OpType type) {
    return  type == Literal ||
            type == NullLiteral ||
            type == TimeLiteral ||
            type == DateLiteral ||
            type == TimestampLiteral;
  }

  /**
   * Check if it is one of function types.
   *
   * @param type The type to be checked
   * @return True if it is aggregation function type. Otherwise, it returns False.
   */
  public static boolean isFunction(OpType type) {
    return type == Function || isAggregationFunction(type) || isWindowFunction(type);
  }

  /**
   * Check if it is an aggregation function type.
   *
   * @param type The type to be checked
   * @return True if it is aggregation function type. Otherwise, it returns False.
   */
  public static boolean isAggregationFunction(OpType type) {
    return type == GeneralSetFunction || type == CountRowsFunction;
  }

  /**
   * Check if it is an window function type.
   *
   * @param type The type to be checked
   * @return True if it is window function type. Otherwise, it returns False.
   */
  public static boolean isWindowFunction(OpType type) {
    return type == WindowFunction;
  }
}
