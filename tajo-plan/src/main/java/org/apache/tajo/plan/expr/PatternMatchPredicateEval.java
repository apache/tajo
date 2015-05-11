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
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Tuple;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public abstract class PatternMatchPredicateEval extends BinaryEval {
  private static final DataType RES_TYPE = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.BOOLEAN);

  @Expose protected boolean not;
  @Expose protected String pattern;
  @Expose protected boolean caseInsensitive;

  // transient variables
  protected Pattern compiled;

  public PatternMatchPredicateEval(EvalType evalType, boolean not, EvalNode predicand, ConstEval pattern,
                                   boolean caseInsensitive) {
    super(evalType, predicand, pattern);
    this.not = not;
    this.pattern = pattern.getValue().asChars();
    this.caseInsensitive = caseInsensitive;
  }

  public PatternMatchPredicateEval(EvalType evalType, boolean not, EvalNode field, ConstEval pattern) {
    this(evalType, not, field, pattern, false);
  }

  abstract void compile(String pattern) throws PatternSyntaxException;

  public boolean isNot() {
    return not;
  }

  public boolean isCaseInsensitive() {
    return caseInsensitive;
  }

  @Override
  public DataType getValueType() {
    return RES_TYPE;
  }

  @Override
  public String getName() {
    return "?";
  }

  @Override
  public EvalNode bind(EvalContext evalContext, Schema schema) {
    super.bind(evalContext, schema);
    compile(pattern);
    return this;
  }

  @Override
  public Datum eval(Tuple tuple) {
    if (!isBound) {
      throw new IllegalStateException("bind() must be called before eval()");
    }
    Datum predicand = leftExpr.eval(tuple);
    if (predicand.isNull()) {
      return NullDatum.get();
    }

    boolean matched = compiled.matcher(predicand.asChars()).matches();
    return DatumFactory.createBool(matched ^ not);
  }
}