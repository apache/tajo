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

import com.google.gson.annotations.Expose;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class RegexPredicateEval extends PatternMatchPredicateEval {
  @Expose private String operator;
  public RegexPredicateEval(boolean not, EvalNode field, ConstEval pattern, boolean caseInsensitive) {
    super(EvalType.REGEX, not, field, pattern, caseInsensitive);
    StringBuilder sb = new StringBuilder();
    if (not) {
      sb.append("!");
    }
    sb.append("~");
    if (caseInsensitive) {
      sb.append("*");
    }
    this.operator = sb.toString();
  }
  
  protected void compile(String regex) throws PatternSyntaxException {
    int flags = Pattern.DOTALL;
    if (caseInsensitive) {
      flags |= Pattern.CASE_INSENSITIVE;
    }
    this.compiled = Pattern.compile(regex, flags);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((operator == null) ? 0 : operator.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    RegexPredicateEval other = (RegexPredicateEval) obj;
    if (operator == null) {
      if (other.operator != null)
        return false;
    } else if (!operator.equals(other.operator))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return leftExpr.toString() + operator + "'" + pattern +"'";
  }
}