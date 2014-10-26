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

package org.apache.tajo.engine.function;

import org.apache.tajo.plan.function.FunctionContext;
import org.junit.Test;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.function.builtin.AvgLong;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import static org.junit.Assert.assertTrue;

public class TestAggFunction {

  @Test
  public void testAvgInt() {
    Tuple [] tuples = new Tuple[5];

    for (int i = 1; i <= 5; i++) {
      tuples[i-1] = new VTuple(1);
      tuples[i-1].put(0, DatumFactory.createInt4(i));
    }

    AvgLong avg = new AvgLong();
    FunctionContext ctx = avg.newContext();
    for (int i = 1; i <= 5; i++) {
      avg.eval(ctx, tuples[i-1]);
    }

    assertTrue(15 / 5 == avg.terminate(ctx).asFloat8());


    Tuple [] tuples2 = new Tuple[10];

    FunctionContext ctx2 = avg.newContext();
    for (int i = 1; i <= 10; i++) {
      tuples2[i-1] = new VTuple(1);
      tuples2[i-1].put(0, DatumFactory.createInt4(i));
      avg.eval(ctx2, tuples2[i-1]);
    }
    assertTrue((double)55 / 10 == avg.terminate(ctx2).asFloat8());


    avg.merge(ctx, new VTuple(new Datum[] {avg.getPartialResult(ctx2)}));
    assertTrue((double)(15 + 55) / (5 + 10) == avg.terminate(ctx).asFloat8());
  }
}
