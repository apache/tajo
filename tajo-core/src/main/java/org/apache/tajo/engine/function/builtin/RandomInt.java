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
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

import java.util.Random;

import static org.apache.tajo.common.TajoDataTypes.Type.INT4;

@Description(
  functionName = "random",
  description = "A pseudorandom number",
  example = "> SELECT random(10);\n"
          + "4",
  returnType = TajoDataTypes.Type.INT4,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.INT4})}
)
public class RandomInt extends GeneralFunction {
  private Random random;

  public RandomInt() {
    super(new Column[] {
        new Column("n", INT4)
    });
    random = new Random(System.nanoTime());
  }

  @Override
  public Datum eval(Tuple params) {
    return DatumFactory.createInt4(random.nextInt(params.getInt4(0)));
  }

}
