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

import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.TUtil;

import java.util.TimeZone;

import static org.apache.tajo.common.TajoDataTypes.DataType;

public class CastEval extends UnaryEval {
  @Expose private DataType target;
  @Expose private TimeZone timezone;

  public CastEval(OverridableConf context, EvalNode operand, DataType target) {
    super(EvalType.CAST, operand);
    this.target = target;

    if (context.containsKey(SessionVars.TIMEZONE)) {
      String timezoneId = context.get(SessionVars.TIMEZONE);
      timezone = TimeZone.getTimeZone(timezoneId);
    }
  }

  public EvalNode getOperand() {
    return child;
  }

  @Override
  public DataType getValueType() {
    return target;
  }

  public boolean hasTimeZone() {
    return this.timezone != null;
  }

  public TimeZone getTimezone() {
    return this.timezone;
  }

  @Override
  public String getName() {
    return target.getType().name();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Datum eval(Tuple tuple) {
    super.eval(tuple);
    Datum operandDatum = child.eval(tuple);
    if (operandDatum.isNull()) {
      return operandDatum;
    }

    return DatumFactory.cast(operandDatum, target, timezone);
  }

  public String toString() {
    return "CAST (" + child + " AS " + target.getType() + ")";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((target == null) ? 0 : target.hashCode());
    result = prime * result + ((timezone == null) ? 0 : timezone.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    boolean valid = obj != null && obj instanceof CastEval;
    if (valid) {
      CastEval another = (CastEval) obj;
      return child.equals(another.child) &&
          target.equals(another.target) &&
          TUtil.checkEquals(timezone, another.timezone);
    } else {
      return false;
    }
  }
}
