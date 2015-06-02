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
import java.security.*;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * Function definition
 *
 * text digest(string text, type text)
 */
@Description(
  functionName = "digest",
  description = "Calculates the Digest hash of string",
  example = "> SELECT digest('tajo', 'sha1');\n"
          + "02b0e20540b89f0b735092bbac8093eb2e3804cf",
  returnType = TajoDataTypes.Type.TEXT,
  paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TEXT})}
)

public class Digest extends GeneralFunction {
  public Digest() {
    super(new Column[] {
        new Column("text", TajoDataTypes.Type.TEXT)
    });
  }

  String digest(byte [] data, String type) throws NoSuchAlgorithmException{
    if ("SHA1".equalsIgnoreCase(type) == true) {
      MessageDigest md = MessageDigest.getInstance("SHA-1");
      return new String(Hex.encodeHex(md.digest(data)));
    } else if ("SHA256".equalsIgnoreCase(type) == true) {
      return DigestUtils.sha256Hex(data);
    } else if ("SHA384".equalsIgnoreCase(type) == true) {
      return DigestUtils.sha384Hex(data);
    } else if ("SHA512".equalsIgnoreCase(type) == true) {
      return DigestUtils.sha512Hex(data);
    } else if ("MD5".equalsIgnoreCase(type) == true) {
      return DigestUtils.md5Hex(data);
    } else if ("MD2".equalsIgnoreCase(type) == true) {
      MessageDigest md = MessageDigest.getInstance("MD2");
      return new String(Hex.encodeHex(md.digest(data)));
    }

    throw new NoSuchAlgorithmException("Not supported DigestType");
  }

  @Override
  public Datum eval(Tuple params) {
    if (params.isBlankOrNull(0) || params.isBlankOrNull(1)) {
      return NullDatum.get();
    }

    try {
        return DatumFactory.createText(digest(params.getBytes(0), params.getText(1)));
    } catch (NoSuchAlgorithmException e){
        return NullDatum.get();
    }
  }
}
