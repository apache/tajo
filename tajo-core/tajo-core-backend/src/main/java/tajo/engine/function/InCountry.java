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

package tajo.engine.function;

import tajo.catalog.Column;
import tajo.catalog.function.GeneralFunction;
import tajo.common.TajoDataTypes;
import tajo.datum.BooleanDatum;
import tajo.datum.Datum;
import tajo.storage.Tuple;
import tajo.util.GeoUtil;

public class InCountry extends GeneralFunction<BooleanDatum> {

  public InCountry() {
    super(new Column[] {new Column("addr", TajoDataTypes.Type.TEXT),
        new Column("code", TajoDataTypes.Type.TEXT)});
  }

  @Override
  public Datum eval(Tuple params) {
    String addr = params.get(0).asChars();
    String otherCode = params.get(1).asChars();
    String thisCode = GeoUtil.getCountryCode(addr);

    return new BooleanDatum(thisCode.equals(otherCode));
  }
}
