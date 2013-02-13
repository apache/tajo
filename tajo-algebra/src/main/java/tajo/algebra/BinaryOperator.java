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

public class BinaryOperator extends Expr {
  Expr left;
  Expr right;

  BinaryOperator(ExprType opType) {
    super(opType);
  }

  public BinaryOperator(ExprType type, Expr left, Expr right) {
    super(type);
    this.left = left;
    this.right = right;
  }

  public Expr getLeft() {
    return this.left;
  }

  public void setLeft(Expr left) {
    this.left = left;
  }

  public Expr getRight() {
    return this.right;
  }

  public void setRight(Expr right) {
    this.right = right;
  }

  @Override
  boolean equalsTo(Expr expr) {
    return true;
  }
}
