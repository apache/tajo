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

import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class PatternMatchPredicate extends BinaryOperator {
  @Expose @SerializedName("IsNot")
  private boolean not;
  @Expose @SerializedName("IsCaseInsensitive")
  private boolean caseInsensitive;

  public PatternMatchPredicate(OpType opType, boolean not, Expr predicand, Expr pattern,
                               boolean caseInsensitive) {
    super(opType);
    Preconditions.checkArgument(
        opType == OpType.LikePredicate || opType == OpType.SimilarToPredicate || opType == OpType.Regexp,
        "pattern matching predicate is only available: " + opType.name());
    this.not = not;
    this.caseInsensitive = caseInsensitive;
    setLeft(predicand);
    setRight(pattern);
  }

  public PatternMatchPredicate(OpType opType, boolean not, Expr predicand, Expr pattern) {
    this(opType, not, predicand, pattern, false);
  }

  public boolean isNot() {
    return not;
  }

  public Expr getPredicand() {
    return getLeft();
  }

  public Expr getPattern() {
    return getRight();
  }

  public boolean isCaseInsensitive() {
    return this.caseInsensitive;
  }

  boolean equalsTo(Expr expr) {
    PatternMatchPredicate another = (PatternMatchPredicate) expr;
    return opType == another.opType &&
        not == another.not && caseInsensitive == another.caseInsensitive;
  }

  @Override
  public PatternMatchPredicate clone() throws CloneNotSupportedException {
    PatternMatchPredicate patternMatchPredicate = (PatternMatchPredicate) super.clone();
    patternMatchPredicate.not = not;
    patternMatchPredicate.caseInsensitive = caseInsensitive;
    return patternMatchPredicate;
  }
}
