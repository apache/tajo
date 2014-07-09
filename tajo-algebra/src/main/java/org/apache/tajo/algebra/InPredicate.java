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

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class InPredicate extends BinaryOperator {
  @Expose @SerializedName("IsNot")
  private boolean not;

  public InPredicate(Expr predicand, Expr in_values, boolean not) {
    super(OpType.InPredicate, predicand, in_values);
    this.not = not;
  }

  public boolean isNot() {
    return this.not;
  }

  public Expr getPredicand() {
    return left;
  }

  public Expr getInValue() {
    return right;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    InPredicate inPredicate = (InPredicate) super.clone();
    inPredicate.not = not;
    return inPredicate;
  }
}
