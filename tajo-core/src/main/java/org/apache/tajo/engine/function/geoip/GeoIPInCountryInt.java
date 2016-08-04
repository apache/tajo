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
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.GeoIPUtil;

@Description(
    functionName = "geoip_in_country",
    description = "If the given country code is same with the country code of the given address, it returns true. "
        + "Otherwise, returns false",
    example = "geoip_in_country(8.8.8.8, 'US')"
        + "true",
    returnType = Type.BOOLEAN,
    paramTypes = {@ParamTypes(paramTypes = {Type.INT4, Type.TEXT}),
        @ParamTypes(paramTypes = {Type.INT8, Type.TEXT})
    }
)
public class GeoIPInCountryInt extends GeneralFunction {
  public GeoIPInCountryInt() {
    super(new Column[] {new Column("ipv4_address", Type.INT8),
        new Column("country_code", Type.TEXT)});
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0) || params.isBlankOrNull(1)) {
      return NullDatum.get();
    }

    int addr = params.getInt4(0);
    String otherCode = params.getText(1);
    String thisCode = GeoIPUtil.getCountryCode(addr);

    return DatumFactory.createBool(thisCode.equals(otherCode));
  }
}
