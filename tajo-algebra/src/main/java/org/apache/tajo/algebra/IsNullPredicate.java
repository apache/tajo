/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.algebra;

public class IsNullPredicate extends Expr {
  private final boolean not;
  private final ColumnReferenceExpr columnRef;

  public IsNullPredicate(boolean not, ColumnReferenceExpr column) {
    super(OpType.IsNullPredicate);
    this.not = not;
    this.columnRef = column;
  }

  public boolean isNot() {
    return not;
  }

  public ColumnReferenceExpr getColumnRef() {
    return columnRef;
  }

  @Override
  boolean equalsTo(Expr expr) {
    IsNullPredicate nullPredicate = (IsNullPredicate) expr;
    return not == nullPredicate.not && columnRef.equals(nullPredicate.columnRef);
  }
}
