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

public class LiteralExpr extends Expr {
  private String value;
  private LiteralType value_type;

  public static enum LiteralType {
    String,
    Unsigned_Integer,
    Unsigned_Float,
    Unsigned_Large_Integer,
  }

  public LiteralExpr(String value, LiteralType value_type) {
    super(ExprType.Literal);
    this.value = value;
    this.value_type = value_type;
  }

  public String getValue() {
    return this.value;
  }

  public LiteralType getValueType() {
    return this.value_type;
  }

  public boolean equalsTo(Expr expr) {
    LiteralExpr another = (LiteralExpr) expr;
    boolean a = value_type.equals(another.value_type);
    boolean b =  value.equals(another.value);

    return a && b;
  }
}
