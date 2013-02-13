/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.algebra;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static tajo.algebra.LiteralExpr.LiteralType;

public class TestExpr {

  @Test
  public void testBinaryEquals() {
    Expr expr1 = new BinaryOperator(ExprType.Plus,
        new LiteralExpr("1", LiteralType.Unsigned_Integer),
        new LiteralExpr("2", LiteralType.Unsigned_Integer));

    Expr expr2 = new BinaryOperator(ExprType.Plus,
        new LiteralExpr("1", LiteralType.Unsigned_Integer),
        new LiteralExpr("2", LiteralType.Unsigned_Integer));

    assertEquals(expr1, expr2);

    Expr expr3 = new BinaryOperator(ExprType.Minus,
        new LiteralExpr("1", LiteralType.Unsigned_Integer),
        new LiteralExpr("2", LiteralType.Unsigned_Integer));

    assertFalse(expr1.equals(expr3));


    Expr expr4 = new BinaryOperator(ExprType.Plus,
        new LiteralExpr("2", LiteralType.Unsigned_Integer),
        new LiteralExpr("2", LiteralType.Unsigned_Integer));

    assertFalse(expr1.equals(expr4));

    Expr expr5 = new BinaryOperator(ExprType.Plus,
        new LiteralExpr("1", LiteralType.Unsigned_Integer),
        new LiteralExpr("3", LiteralType.Unsigned_Integer));

    assertFalse(expr1.equals(expr5));
  }

  @Test
  public void testBinaryHierarchyEquals() {
    Expr left1 = new BinaryOperator(ExprType.Plus,
        new LiteralExpr("1", LiteralType.Unsigned_Integer),
        new LiteralExpr("2", LiteralType.Unsigned_Integer));

    Expr right1 = new BinaryOperator(ExprType.Multiply,
        new LiteralExpr("2", LiteralType.Unsigned_Integer),
        new LiteralExpr("3", LiteralType.Unsigned_Integer));

    Expr one = new BinaryOperator(ExprType.Plus, left1, right1);

    Expr left2 = new BinaryOperator(ExprType.Plus,
        new LiteralExpr("1", LiteralType.Unsigned_Integer),
        new LiteralExpr("2", LiteralType.Unsigned_Integer));

    Expr right2 = new BinaryOperator(ExprType.Multiply,
        new LiteralExpr("2", LiteralType.Unsigned_Integer),
        new LiteralExpr("3", LiteralType.Unsigned_Integer));

    Expr two = new BinaryOperator(ExprType.Plus, left2, right2);

    assertEquals(one, two);

    Expr left3 = new BinaryOperator(ExprType.Minus,
        new LiteralExpr("1", LiteralType.Unsigned_Integer),
        new LiteralExpr("2", LiteralType.Unsigned_Integer));

    Expr wrong1 = new BinaryOperator(ExprType.Plus, left3, right1);

    assertFalse(one.equals(wrong1));
  }

  @Test
  public void testEquals() throws Exception {
    Expr expr = new BinaryOperator(ExprType.LessThan,
        new LiteralExpr("1", LiteralType.Unsigned_Integer),
        new LiteralExpr("2", LiteralType.Unsigned_Integer));

    Relation relation = new Relation("employee");
    Selection selection = new Selection(relation, expr);

    Aggregation aggregation = new Aggregation();
    aggregation.setTargets(new Target[]{
          new Target(new ColumnReferenceExpr("col1"))
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
    Expr expr = new BinaryOperator(ExprType.LessThan,
        new LiteralExpr("1", LiteralType.Unsigned_Integer),
        new LiteralExpr("2", LiteralType.Unsigned_Integer));

    Relation relation = new Relation("employee");
    Selection selection = new Selection(relation, expr);

    Aggregation aggregation = new Aggregation();
    aggregation.setTargets(new Target[]{
        new Target(new ColumnReferenceExpr("col1"))
    });

    aggregation.setChild(selection);

    Sort.SortSpec spec = new Sort.SortSpec(new ColumnReferenceExpr("col2"), false, false);
    Sort sort = new Sort(new Sort.SortSpec[]{spec});
    sort.setChild(aggregation);

    return sort;
  }
}
