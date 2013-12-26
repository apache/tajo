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

package org.apache.tajo.engine.eval;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class LikePredicateEval extends PatternMatchPredicateEval {
  private static final String LIKE_ESCAPE_SPATIAL_CHARACTERS = "([.*${}?|\\^\\-\\[\\]])";

  public LikePredicateEval(boolean not, EvalNode field, ConstEval pattern, boolean caseSensitive) {
    super(EvalType.LIKE, not, field, pattern, caseSensitive);
  }

  private String escapeRegexpForLike(String literal) {
    return literal.replaceAll(LIKE_ESCAPE_SPATIAL_CHARACTERS, "\\\\$1");
  }
  
  protected void compile(String pattern) throws PatternSyntaxException {
    String escaped = escapeRegexpForLike(pattern);
    String regex = escaped.replace("_", ".").replace("%", ".*");
    int flags = Pattern.DOTALL;
    if (caseInsensitive) {
      flags |= Pattern.CASE_INSENSITIVE;
    }
    this.compiled = Pattern.compile(regex, flags);
  }

  public boolean isLeadingWildCard() {
    return pattern.indexOf(".*") == 0;
  }

  @Override
  public String toString() {
    return leftExpr.toString() + (caseInsensitive ? "ILIKE" : "LIKE") + "'" + pattern +"'";
  }
}