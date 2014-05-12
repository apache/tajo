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

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static org.apache.tajo.algebra.LiteralValue.LiteralType;

public class TestExpr {

  @Test
  public void testBinaryEquals() {
    Expr expr1 = new BinaryOperator(OpType.Plus,
        new LiteralValue("1", LiteralType.Unsigned_Integer),
        new LiteralValue("2", LiteralType.Unsigned_Integer));

    Expr expr2 = new BinaryOperator(OpType.Plus,
        new LiteralValue("1", LiteralType.Unsigned_Integer),
        new LiteralValue("2", LiteralType.Unsigned_Integer));

    assertEquals(expr1.hashCode(), expr2.hashCode());
    assertEquals(expr1, expr2);

    Expr expr3 = new BinaryOperator(OpType.Minus,
        new LiteralValue("1", LiteralType.Unsigned_Integer),
        new LiteralValue("2", LiteralType.Unsigned_Integer));

    assertFalse(expr1.equals(expr3));


    Expr expr4 = new BinaryOperator(OpType.Plus,
        new LiteralValue("2", LiteralType.Unsigned_Integer),
        new LiteralValue("2", LiteralType.Unsigned_Integer));

    assertFalse(expr1.equals(expr4));

    Expr expr5 = new BinaryOperator(OpType.Plus,
        new LiteralValue("1", LiteralType.Unsigned_Integer),
        new LiteralValue("3", LiteralType.Unsigned_Integer));

    assertFalse(expr1.equals(expr5));
  }

  @Test
  public void testBinaryHierarchyEquals() {
    Expr left1 = new BinaryOperator(OpType.Plus,
        new LiteralValue("1", LiteralType.Unsigned_Integer),
        new LiteralValue("2", LiteralType.Unsigned_Integer));

    Expr right1 = new BinaryOperator(OpType.Multiply,
        new LiteralValue("2", LiteralType.Unsigned_Integer),
        new LiteralValue("3", LiteralType.Unsigned_Integer));

    Expr one = new BinaryOperator(OpType.Plus, left1, right1);

    Expr left2 = new BinaryOperator(OpType.Plus,
        new LiteralValue("1", LiteralType.Unsigned_Integer),
        new LiteralValue("2", LiteralType.Unsigned_Integer));

    Expr right2 = new BinaryOperator(OpType.Multiply,
        new LiteralValue("2", LiteralType.Unsigned_Integer),
        new LiteralValue("3", LiteralType.Unsigned_Integer));

    Expr two = new BinaryOperator(OpType.Plus, left2, right2);

    assertEquals(one, two);

    Expr left3 = new BinaryOperator(OpType.Minus,
        new LiteralValue("1", LiteralType.Unsigned_Integer),
        new LiteralValue("2", LiteralType.Unsigned_Integer));

    Expr wrong1 = new BinaryOperator(OpType.Plus, left3, right1);

    assertFalse(one.equals(wrong1));
  }

  @Test
  public void testEquals() throws Exception {
    Expr expr = new BinaryOperator(OpType.LessThan,
        new LiteralValue("1", LiteralType.Unsigned_Integer),
        new LiteralValue("2", LiteralType.Unsigned_Integer));

    Relation relation = new Relation("employee");
    Selection selection = new Selection(expr);
    selection.setChild(relation);

    Aggregation aggregation = new Aggregation();
    aggregation.setTargets(new NamedExpr[]{
          new NamedExpr(new ColumnReferenceExpr("col1"))
        }
    );

    aggregation.setChild(selection);

    Sort.SortSpec spec = new Sort.SortSpec(new ColumnReferenceExpr("col2"));
    Sort sort = new Sort(new Sort.SortSpec[]{spec});
    sort.setChild(aggregation);

    assertEquals(sort, sort);

    Expr different = generateOneExpr();
    assertFalse(sort.equals(different));
  }

  private Expr generateOneExpr() {
    Expr expr = new BinaryOperator(OpType.LessThan,
        new LiteralValue("1", LiteralType.Unsigned_Integer),
        new LiteralValue("2", LiteralType.Unsigned_Integer));

    Relation relation = new Relation("employee");
    Selection selection = new Selection(expr);
    selection.setChild(relation);

    Aggregation aggregation = new Aggregation();
    aggregation.setTargets(new NamedExpr[]{
        new NamedExpr(new ColumnReferenceExpr("col1"))
    });

    aggregation.setChild(selection);

    Sort.SortSpec spec = new Sort.SortSpec(new ColumnReferenceExpr("col2"), false, false);
    Sort sort = new Sort(new Sort.SortSpec[]{spec});
    sort.setChild(aggregation);

    return sort;
  }

  @Test
  public void testJson() {
    Expr left1 = new BinaryOperator(OpType.Plus,
        new LiteralValue("1", LiteralType.Unsigned_Integer),
        new LiteralValue("2", LiteralType.Unsigned_Integer));

    Expr right1 = new BinaryOperator(OpType.Multiply,
        new LiteralValue("2", LiteralType.Unsigned_Integer),
        new LiteralValue("3", LiteralType.Unsigned_Integer));

    Expr origin = new BinaryOperator(OpType.Plus, left1, right1);
    String json = origin.toJson();
    Expr fromJson = JsonHelper.fromJson(json, Expr.class);
    assertEquals(origin, fromJson);
  }

  @Test
  public void testJson2() {
    Expr expr = new BinaryOperator(OpType.LessThan,
        new LiteralValue("1", LiteralType.Unsigned_Integer),
        new LiteralValue("2", LiteralType.Unsigned_Integer));

    Relation relation = new Relation("employee");
    Selection selection = new Selection(expr);
    selection.setChild(relation);

    Aggregation aggregation = new Aggregation();
    aggregation.setTargets(new NamedExpr[]{
            new NamedExpr(new ColumnReferenceExpr("col1"))
        }
    );

    aggregation.setChild(selection);

    Sort.SortSpec spec = new Sort.SortSpec(new ColumnReferenceExpr("col2"));
    Sort sort = new Sort(new Sort.SortSpec[]{spec});
    sort.setChild(aggregation);

    String json = sort.toJson();
    Expr fromJson = JsonHelper.fromJson(json, Expr.class);
    assertEquals(sort, fromJson);
  }
}
