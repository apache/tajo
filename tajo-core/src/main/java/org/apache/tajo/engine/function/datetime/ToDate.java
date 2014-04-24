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

package org.apache.tajo.engine.function.datetime;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.DateDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Description(
    functionName = "to_date",
    description = "Convert string to date. Format should be a java format string.",
    example = "> SELECT to_date('2014-01-01', 'yyyy-MM-dd');\n"
        + "2014-01-01",
    returnType = TajoDataTypes.Type.DATE,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT})}
)
public class ToDate extends GeneralFunction {
  private static Map<String, DateTimeFormatter> formattercCache =
      new ConcurrentHashMap<String, DateTimeFormatter>();

  public ToDate() {
    super(new Column[]{
        new Column("string", TajoDataTypes.Type.TEXT),
        new Column("format", TajoDataTypes.Type.TEXT)});
  }

  @Override
  public Datum eval(Tuple params) {
    if(params.isNull(0) || params.isNull(1)) {
      return NullDatum.get();
    }

    String value = params.get(0).asChars();
    String pattern = params.get(1).asChars();

    DateTimeFormatter formatter = formattercCache.get(pattern);
    if (formatter == null) {
      formatter = DateTimeFormat.forPattern(pattern);
      formattercCache.put(pattern, formatter);
    }

    return new DateDatum(formatter.parseDateTime(value).toLocalDate());
  }
}
