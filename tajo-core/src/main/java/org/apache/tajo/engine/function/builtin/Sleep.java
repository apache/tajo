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

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

@Description(
  functionName = "sleep",
  description = "sleep for seconds",
  example = "> SELECT sleep(1) from table1;",
  returnType = TajoDataTypes.Type.INT4,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.INT4})}
)
public class Sleep extends GeneralFunction {

  public Sleep() {
    super(NoArgs);
  }

  @Override
  public Datum eval(final Tuple params) {

    Runnable sleep = new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(params.getInt4(0) * 1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    sleep.run();

    return DatumFactory.createInt4(params.getInt4(0));
  }
}
