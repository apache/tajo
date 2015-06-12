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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.Int8Datum;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;

@Description(
  functionName = "date",
  description = "Extracts the date part of the date or datetime expression expr.",
  example = "> SELECT date(expr);",
  returnType = TajoDataTypes.Type.INT8,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.INT4})}
)
public class Date extends GeneralFunction {
  private final Log LOG = LogFactory.getLog(Date.class);
  private final static String dateFormat = "dd/MM/yyyy HH:mm:ss";

  public Date() {
    super(new Column[] {new Column("expr", TEXT)});
  }

  @Override
  public Int8Datum eval(Tuple params) {
    try {
      return DatumFactory.createInt8(new SimpleDateFormat(dateFormat)
          .parse(params.getText(0)).getTime());
    } catch (ParseException e) {
      LOG.error(e);
      return null;
    }
  }
}
