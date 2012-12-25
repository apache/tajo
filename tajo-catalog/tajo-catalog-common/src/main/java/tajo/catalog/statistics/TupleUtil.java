/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.catalog.statistics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.catalog.proto.CatalogProtos;
import tajo.datum.*;

public class TupleUtil {
  /** class logger **/
  private static final Log LOG = LogFactory.getLog(TupleUtil.class);

  public static Datum createFromBytes(CatalogProtos.DataType type, byte [] bytes) {
    switch (type) {
      case BOOLEAN:
        return new BoolDatum(bytes);
      case BYTE:
        return new ByteDatum(bytes);
      case CHAR:
        return new CharDatum(bytes);
      case SHORT:
        return new ShortDatum(bytes);
      case INT:
        return new IntDatum(bytes);
      case LONG:
        return new LongDatum(bytes);
      case FLOAT:
        return new FloatDatum(bytes);
      case DOUBLE:
        return new DoubleDatum(bytes);
      case STRING:
        return new StringDatum(bytes);
      case IPv4:
        return new IPv4Datum(bytes);
      default: throw new UnsupportedOperationException(type + " is not supported yet");
    }
  }
}
