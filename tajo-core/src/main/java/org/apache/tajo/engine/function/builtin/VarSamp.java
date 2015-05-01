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
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.function.FunctionContext;

public abstract class VarSamp extends Variance {
  public VarSamp(Column[] definedArgs) {
    super(definedArgs);
  }

  @Override
  public Datum terminate(FunctionContext ctx) {
    VarianceContext varianceCtx = (VarianceContext) ctx;
    if (varianceCtx.count <= 1) {
      return NullDatum.get();
    }
    return DatumFactory.createFloat8(varianceCtx.squareSumOfDiff / (varianceCtx.count - 1));
  }
}
