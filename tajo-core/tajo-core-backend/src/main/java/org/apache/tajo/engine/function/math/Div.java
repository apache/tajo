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

package org.apache.tajo.engine.function.math;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;

/**
 * Function definition
 *
 * INT8 div(value INT8, value INT8)
 */
public class Div extends GeneralFunction {
  public Div() {
    super(new Column[] {
      new Column("value1", TajoDataTypes.Type.INT8),
      new Column("value2", TajoDataTypes.Type.INT8)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    Datum value1Datum = params.get(0);
    if(value1Datum instanceof NullDatum) {
      return NullDatum.get();
    }

    Datum value2Datum = params.get(1);
    if(value2Datum instanceof NullDatum) {
      return NullDatum.get();
    }

    long value1 = value1Datum.asInt8();
    long value2 = value2Datum.asInt8();

    if (value2 == 0) {
      return NullDatum.get();
    }

    return DatumFactory.createInt8(value1/value2);
  }
}
