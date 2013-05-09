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

package tajo.engine.function.builtin;

import tajo.catalog.Column;
import tajo.catalog.function.GeneralFunction;
import tajo.datum.Datum;
import tajo.datum.DatumFactory;
import tajo.storage.Tuple;

import java.util.Random;

import static tajo.common.TajoDataTypes.Type.INT4;

public class RandomInt extends GeneralFunction<Datum> {
  private Random random;

  public RandomInt() {
    super(new Column[] {
        new Column("val", INT4)
    });
    random = new Random(System.nanoTime());
  }

  @Override
  public Datum eval(Tuple params) {
    return DatumFactory.createInt4(random.nextInt(params.get(0).asInt4()));
  }
}
