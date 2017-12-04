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
import org.apache.tajo.datum.BlobDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.IPconvertUtil;

import static org.apache.tajo.common.TajoDataTypes.Type.BINARY;
import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;

@Description(
    functionName = "ipstr_to_blob",
    description = "Convert an ipv4 address string to BINARY type",
    example = "N/A",
    returnType = BINARY,
    paramTypes = {@ParamTypes(paramTypes = {TEXT})}
)
public class IPstrToBlob extends GeneralFunction {
  public IPstrToBlob() {
    super(new Column[] { new Column("ipstring", TEXT)});
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0)) {
      return NullDatum.get();
    }

    String ipstr = params.getText(0);

    return new BlobDatum(IPconvertUtil.ipstr2bytes(ipstr));
  }
}
