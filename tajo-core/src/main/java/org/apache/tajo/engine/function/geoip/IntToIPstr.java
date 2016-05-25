/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.tajo.engine.function.geoip;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.Int4Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.GeoIPUtil;

import static org.apache.tajo.common.TajoDataTypes.Type.INT4;
import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;

@Description(
    functionName = "int_to_ipstr",
    description = "Convert an INT4 type value to ipv4 string",
    example = "> SELECT int_to_ipstr(16909060);\n"
        + "1.2.3.4",
    returnType = TEXT,
    paramTypes = {@ParamTypes(paramTypes = {INT4})}
)
public class IntToIPstr extends GeneralFunction {
  public IntToIPstr() {
    super(new Column[] { new Column("ip_intvalue", INT4)});
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0)) {
      return NullDatum.get();
    }

    int ipaddr = params.getInt4(0);

    return new TextDatum(GeoIPUtil.int2ipstr(ipaddr));
  }
}
