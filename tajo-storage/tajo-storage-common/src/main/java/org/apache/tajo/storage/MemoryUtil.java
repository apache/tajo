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

package org.apache.tajo.storage;

import org.apache.tajo.datum.*;
import org.apache.tajo.util.ClassSize;

public class MemoryUtil {

  /** Overhead for an NullDatum */
  public static final long NULL_DATUM;

  /** Overhead for an BoolDatum */
  public static final long BOOL_DATUM;

  /** Overhead for an CharDatum */
  public static final long CHAR_DATUM;

  /** Overhead for an BitDatum */
  public static final long BIT_DATUM;

  /** Overhead for an Int2Datum */
  public static final long INT2_DATUM;

  /** Overhead for an Int4Datum */
  public static final long INT4_DATUM;

  /** Overhead for an Int8Datum */
  public static final long INT8_DATUM;

  /** Overhead for an Float4Datum */
  public static final long FLOAT4_DATUM;

  /** Overhead for an Float8Datum */
  public static final long FLOAT8_DATUM;

  /** Overhead for an TextDatum */
  public static final long TEXT_DATUM;

  /** Overhead for an BlobDatum */
  public static final long BLOB_DATUM;

  /** Overhead for an DateDatum */
  public static final long DATE_DATUM;

  /** Overhead for an TimeDatum */
  public static final long TIME_DATUM;

  /** Overhead for an TimestampDatum */
  public static final long TIMESTAMP_DATUM;

  static {
    NULL_DATUM = ClassSize.estimateBase(NullDatum.class, false);

    CHAR_DATUM = ClassSize.estimateBase(CharDatum.class, false);

    BOOL_DATUM = ClassSize.estimateBase(BooleanDatum.class, false);

    BIT_DATUM = ClassSize.estimateBase(BitDatum.class, false);

    INT2_DATUM = ClassSize.estimateBase(Int2Datum.class, false);

    INT4_DATUM = ClassSize.estimateBase(Int4Datum.class, false);

    INT8_DATUM = ClassSize.estimateBase(Int8Datum.class, false);

    FLOAT4_DATUM = ClassSize.estimateBase(Float4Datum.class, false);

    FLOAT8_DATUM = ClassSize.estimateBase(Float8Datum.class, false);

    TEXT_DATUM = ClassSize.estimateBase(TextDatum.class, false);

    BLOB_DATUM = ClassSize.estimateBase(BlobDatum.class, false);

    DATE_DATUM = ClassSize.estimateBase(DateDatum.class, false);

    TIME_DATUM = ClassSize.estimateBase(TimeDatum.class, false);

    TIMESTAMP_DATUM = ClassSize.estimateBase(TimestampDatum.class, false);
  }

  public static long calculateMemorySize(Tuple tuple) {
    long total = ClassSize.OBJECT;
    for (Datum datum : tuple.getValues()) {
      switch (datum.type()) {

      case NULL_TYPE:
        total += NULL_DATUM;
        break;

      case BOOLEAN:
        total += BOOL_DATUM;
        break;

      case BIT:
        total += BIT_DATUM;
        break;

      case CHAR:
        total += CHAR_DATUM + datum.size();
        break;

      case INT1:
      case INT2:
        total += INT2_DATUM;
        break;

      case INT4:
        total += INT4_DATUM;
        break;

      case INT8:
        total += INT8_DATUM;
        break;

      case FLOAT4:
        total += FLOAT4_DATUM;
        break;

      case FLOAT8:
        total += FLOAT4_DATUM;
        break;

      case TEXT:
        total += TEXT_DATUM + datum.size();
        break;

      case BLOB:
        total += BLOB_DATUM + datum.size();
        break;

      case DATE:
        total += DATE_DATUM;
        break;

      case TIME:
        total += TIME_DATUM;
        break;

      case TIMESTAMP:
        total += TIMESTAMP_DATUM;
        break;

      default:
        break;
      }
    }

    return total;
  }
}
