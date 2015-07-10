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
package org.apache.tajo.engine.function.string;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;


/**
 * Function definition
 * text ToBin(int n) - returns n in binary
 *   Example:
 *   SELECT Tobin(22) FROM src LIMIT 1;
 *   -> result: '10110'
 */
@Description(
  functionName = "to_bin",
  description = "Returns n in binary.",
  example = "> SELECT to_bin(22);\n"
      + "10110",
  returnType = TajoDataTypes.Type.TEXT,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.INT8}),
      @ParamTypes(paramTypes = {TajoDataTypes.Type.INT4})}
)
public class ToBin extends GeneralFunction {

  public ToBin() {
    super(new Column[] {
        new Column("n", TajoDataTypes.Type.INT8)
    });
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0)) {
      return NullDatum.get();
    }

    return DatumFactory.createText(Long.toBinaryString(params.getInt8(0)));
  }

}
