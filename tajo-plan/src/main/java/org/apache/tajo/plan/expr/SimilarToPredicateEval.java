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

package org.apache.tajo.plan.expr;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class SimilarToPredicateEval extends PatternMatchPredicateEval {
  private static final String SIMILARTO_ESCAPE_SPATIAL_CHARACTERS = "([.])";

  public SimilarToPredicateEval(boolean not, EvalNode field, ConstEval pattern,
                                @SuppressWarnings("unused") boolean isCaseSensitive) {
    super(EvalType.SIMILAR_TO, not, field, pattern, false);
  }

  public SimilarToPredicateEval(boolean not, EvalNode field, ConstEval pattern) {
    super(EvalType.SIMILAR_TO, not, field, pattern);
  }

  @Override
  protected void compile(String pattern) throws PatternSyntaxException {
    String regex = pattern.replaceAll(SIMILARTO_ESCAPE_SPATIAL_CHARACTERS, "\\\\$1");
    regex = regex.replace("_", ".").replace("%", ".*"); // transform some special characters to be 'like'.

    this.compiled = Pattern.compile(regex, Pattern.DOTALL);
  }
  
  @Override
  public String toString() {
    return leftExpr.toString() + " SIMILAR TO '" + pattern + "'";
  }
}