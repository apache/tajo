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

public class LikePredicate extends Expr {
  private boolean not;
  private ColumnReferenceExpr columnRef;
  private Expr pattern;

  public LikePredicate(boolean not, ColumnReferenceExpr columnReferenceExpr, Expr pattern) {
    super(OpType.LikePredicate);
    this.not = not;
    this.columnRef = columnReferenceExpr;
    this.pattern = pattern;
  }

  public boolean isNot() {
    return not;
  }

  public ColumnReferenceExpr getColumnRef() {
    return this.columnRef;
  }

  public Expr getPattern() {
    return this.pattern;
  }

  boolean equalsTo(Expr expr) {
    LikePredicate another = (LikePredicate) expr;
    return not == another.not &&
        columnRef.equals(another.columnRef) &&
        pattern.equals(another.pattern);
  }
}
