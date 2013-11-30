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
import org.apache.tajo.engine.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;

/**
 * Function definition
 *
 * text substr(string text, from int4 [, length int4])
 */
public class Substr extends GeneralFunction {
  public Substr() {
    super(new Column[] {
        new Column("text", TajoDataTypes.Type.TEXT),
        new Column("from", TajoDataTypes.Type.INT4),
        new Column("length", TajoDataTypes.Type.INT4)    //optional
    });
  }

  @Override
  public Datum eval(Tuple params) {
    Datum valueDatum = params.get(0);
    if(valueDatum instanceof NullDatum) {
      return NullDatum.get();
    }
    Datum fromDatum = params.get(1);
    Datum lengthDatum = params.size() > 2 ? params.get(2) : null;

    String value = valueDatum.asChars();
    int valueLength = value.length();

    int from = fromDatum.asInt4() - 1;
    if (from >= valueLength) {
      return DatumFactory.createText("");
    }

    int length = (lengthDatum == null) ? valueLength : lengthDatum.asInt4();

    if (from < 0) {
      from = 0;
      length = (lengthDatum == null) ? value.length() : length - 1;
    }

    int to = from + length;

    if (to > valueLength) {
      to = valueLength;
    }

    return DatumFactory.createText(value.substring(from, to));
  }
}
