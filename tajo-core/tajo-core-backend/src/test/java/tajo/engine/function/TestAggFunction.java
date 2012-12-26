/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.engine.function;

import org.junit.Test;
import tajo.catalog.function.FunctionContext;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.engine.function.builtin.AvgLong;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import static org.junit.Assert.assertTrue;

/**
 * @author Hyunsik Choi
 */
public class TestAggFunction {

  @Test
  public void testAvgInt() {
    Tuple [] tuples = new Tuple[5];

    for (int i = 1; i <= 5; i++) {
      tuples[i-1] = new VTuple(1);
      tuples[i-1].put(0, DatumFactory.createInt(i));
    }

    AvgLong avg = new AvgLong();
    FunctionContext ctx = avg.newContext();
    for (int i = 1; i <= 5; i++) {
      avg.eval(ctx, tuples[i-1]);
    }

    assertTrue(15 / 5 == avg.terminate(ctx).asDouble());


    Tuple [] tuples2 = new Tuple[10];

    FunctionContext ctx2 = avg.newContext();
    for (int i = 1; i <= 10; i++) {
      tuples2[i-1] = new VTuple(1);
      tuples2[i-1].put(0, DatumFactory.createInt(i));
      avg.eval(ctx2, tuples2[i-1]);
    }
    assertTrue((double)55 / 10 == avg.terminate(ctx2).asDouble());


    avg.merge(ctx, new VTuple(new Datum[] {avg.getPartialResult(ctx2)}));
    assertTrue((double)(15 + 55) / (5 + 10) == avg.terminate(ctx).asDouble());
  }
}
