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

package org.apache.tajo.engine.function.builtin;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.function.AggFunction;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.storage.Tuple;

import java.util.LinkedList;

public abstract class Lead extends AggFunction<Datum> {

  public Lead(Column[] columns) {
    super(columns);
  }

  @Override
  public FunctionContext newContext() {
    return new LeadContext();
  }

  @Override
  public void eval(FunctionContext ctx, Tuple params) {
    LeadContext leadCtx = (LeadContext)ctx;
    if (leadCtx.leadNum < 0) {
      if (params.size() == 1) {
        leadCtx.leadNum = 1;
      } else {
        leadCtx.leadNum = params.getInt4(1);
      }
    }

    if (leadCtx.leadNum > 0) {
      leadCtx.leadNum --;
    } else {
      leadCtx.leadBuffer.add(params.asDatum(0));
    }

    if (leadCtx.defaultDatum == null) {
      if (params.size() == 3) {
        leadCtx.defaultDatum = params.asDatum(2);
      } else {
        leadCtx.defaultDatum = NullDatum.get();
      }
    }
  }

  @Override
  public Datum getPartialResult(FunctionContext ctx) {
    LeadContext leadCtx = (LeadContext)ctx;
    if (leadCtx.leadBuffer.isEmpty()) {
      return leadCtx.defaultDatum;
    } else {
      return leadCtx.leadBuffer.removeFirst();
    }
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    LeadContext leadCtx = (LeadContext)ctx;
    if (leadCtx.leadBuffer.isEmpty()) {
      return leadCtx.defaultDatum;
    } else {
      return leadCtx.leadBuffer.removeFirst();
    }
  }

  private static class LeadContext implements FunctionContext {
    LinkedList<Datum> leadBuffer = new LinkedList<Datum>();
    int leadNum = -1;
    Datum defaultDatum = null;
  }
}
