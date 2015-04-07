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

package org.apache.tajo.engine.function.dataformat;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

import java.text.DecimalFormat;

/**
 * Function definition
 *
 * text to_char(numeric "any", str "pattern")
 */
@Description(
    functionName = "to_char_java",
    description = "convert number to string.",
    detail = "In a to_char output template string, there are certain patterns that are recognized and replaced with appropriately-formatted data based on the given value.",
    example = "> SELECT to_java_char(123, '999');\n"
        + "123",
    returnType = TajoDataTypes.Type.TEXT,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.INT4}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.INT8}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT4}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT8}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.INT4, TajoDataTypes.Type.TEXT}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.INT8, TajoDataTypes.Type.TEXT}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT4, TajoDataTypes.Type.TEXT}),
        @ParamTypes(paramTypes = {TajoDataTypes.Type.FLOAT8, TajoDataTypes.Type.TEXT})
    }
)
public class ToCharJavaDataFormat extends GeneralFunction {


  public ToCharJavaDataFormat() {
    super(new Column[] {
        new Column("number", TajoDataTypes.Type.NUMERIC),
        new Column("pattern", TajoDataTypes.Type.TEXT)
    });
  }

  public void evaluate(String num, StringBuilder result) {
    result.append(num);
  }

  public void evaluate(String num, String pttn, DecimalFormat decimalFormat, StringBuilder result) {
    double number = Double.parseDouble(num);
    //String pattern = pttn.replace("9", "#");
    decimalFormat.applyPattern(pttn);
    result.append(decimalFormat.format(number));
  }

  @Override
  public Datum eval(Tuple params) {
    Datum number = params.get(0);
    Datum pattern = null;

    DecimalFormat decimalFormat = new DecimalFormat();
    StringBuilder result = new StringBuilder();

    String num = "";
    String pttn = "";

    if(number instanceof NullDatum) {
      return NullDatum.get();
    }

    if(params.size() == 1) {
      num = number.asChars();
      this.evaluate(num, result);
    }
    else {
      pattern = params.get(1);
      num = number.asChars();
      pttn = pattern.asChars();
      this.evaluate(num, pttn, decimalFormat, result);
    }

    return DatumFactory.createText(result.toString());
  }
}