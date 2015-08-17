/*
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

package org.apache.tajo.engine.function;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;

@Description(
    functionName = "fail",
    description = "Cause some error if the second parameter value is greater than the first parameter",
    example = "> SELECT fail(1, col1, 'message');\n"
        + "1",
    returnType = Type.INT4,
    paramTypes = {
        @ParamTypes(paramTypes = {Type.INT4, Type.INT4, Type.TEXT}),
    }
)
public class FailFunction extends GeneralFunction {

  public FailFunction() {
    super(new Column[]{
        new Column("trigger", TajoDataTypes.Type.INT4),
        new Column("input_number", TajoDataTypes.Type.INT4),
        new Column("message", TajoDataTypes.Type.TEXT)
    });
  }

  @Override
  public Datum eval(Tuple params) {

    // to skip the plannin phase
    if (params.isBlankOrNull(0) && params.isBlankOrNull(1)) {
      return DatumFactory.createInt4(params.getInt4(0));
    }

    final int trigger = params.getInt4(0);
    final int num = params.getInt4(1);
    final String msg = params.getText(2);

    if (num >= trigger) {
      throw new TajoInternalError(msg);
    }

    return DatumFactory.createInt4(params.getInt4(0));
  }
}
