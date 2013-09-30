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

import org.apache.commons.lang.StringUtils;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.engine.function.GeneralFunction;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Tuple;

/**
 * Function definition
 *
 * text split_part(string text, delimiter text, field int)
 */
public class SplitPart extends GeneralFunction {
  public SplitPart() {
    super(new Column[] {
        new Column("text", TajoDataTypes.Type.TEXT),
        new Column("delimiter", TajoDataTypes.Type.TEXT),
        new Column("field", TajoDataTypes.Type.INT4),
    });
  }

  @Override
  public Datum eval(Tuple params) {
    Datum datum = params.get(0);
    if(datum instanceof NullDatum) return NullDatum.get();

    String [] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(datum.asChars(), params.get(1).asChars(), -1);
    int idx = params.get(2).asInt4() - 1;
    if (split.length > idx) {
      return DatumFactory.createText(split[idx]);
    } else {
      return NullDatum.get();
    }
  }
}
