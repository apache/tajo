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
  Projection(Projection.class), // 0
  Limit(Limit.class), // 1
  Sort(Sort.class), // 2
  Having(Having.class), // 3
  Aggregation(Aggregation.class), // 4
  Join(Join.class), // 5
  Filter(Selection.class), // 6
  Union(SetOperation.class), // 7
  Except(SetOperation.class), // 8
  Intersect(SetOperation.class), // 9
  SimpleTableSubQuery(SimpleTableSubQuery.class), // 10
  TablePrimaryTableSubQuery(TablePrimarySubQuery.class), // 11
  RelationList(RelationList.class), // 12
  Relation(Relation.class), // 13

  // Data definition language
  CreateTable(CreateTable.class),
  DropTable(DropTable.class),

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
  Target(TargetExpr.class),
  Function(FunctionExpr.class),

  // Set Functions
  CountRowsFunction(CountRowsFunctionExpr.class),
  GeneralSetFunction(GeneralSetFunctionExpr.class),

  // Literal
  Cast(CastExpr.class),
  ScalarSubQuery(ScalarSubQuery.class),
  Literal(LiteralValue.class),
  Null(NullValue.class),
  DataType(DataTypeExpr.class);

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
}