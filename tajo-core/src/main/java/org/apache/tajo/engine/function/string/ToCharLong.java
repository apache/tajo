/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;

import java.text.DecimalFormat;

@Description(
    functionName = "to_char",
    description = "convert integer to string.",
    example = "> SELECT to_char(125, '00999');\n"
        + "00125",
    returnType = TajoDataTypes.Type.TEXT,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.INT8, TajoDataTypes.Type.TEXT})}
)

public class ToCharLong extends GeneralFunction {
  DecimalFormat df = null;

  public ToCharLong() {
    super(new Column[]{new Column("val", TajoDataTypes.Type.INT8), new Column("format", TajoDataTypes.Type.TEXT)});
  }

  @Override
  public Datum eval(Tuple params) {
    if (df == null) {
      df = new DecimalFormat(params.getText(1));
    }
    return new TextDatum(df.format(params.getInt8(0)));
  }
}