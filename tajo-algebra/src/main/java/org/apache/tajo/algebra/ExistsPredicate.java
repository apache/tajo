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

public class ExistsPredicate extends Expr {
  private SimpleTableSubQuery simpleTableSubQuery;
  private boolean not;

  public ExistsPredicate(SimpleTableSubQuery simpleTableSubQuery, boolean not) {
    super(OpType.InPredicate);
    this.simpleTableSubQuery = simpleTableSubQuery;
    this.not = not;
  }

  public boolean isNot() {
    return this.not;
  }

  public SimpleTableSubQuery getSubQuery() {
    return simpleTableSubQuery;
  }

  @Override
  boolean equalsTo(Expr expr) {
    ExistsPredicate another = (ExistsPredicate) expr;
    return not == another.not && simpleTableSubQuery.equals(another.simpleTableSubQuery);
  }
}
