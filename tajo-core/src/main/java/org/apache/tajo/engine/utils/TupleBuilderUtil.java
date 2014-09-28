/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tajo.engine.utils;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.TupleBuilder;
import org.apache.tajo.tuple.offheap.RowWriter;

public class TupleBuilderUtil {

  public static void evaluate(Schema inSchema, Tuple input, RowWriter builder, EvalNode[] evals) {
    builder.startRow();
    for (int i = 0; i < evals.length; i++) {
      Datum result = evals[i].eval(inSchema, input);
      writeEvalResult(builder, result.type(), result);
    }
    builder.endRow();
  }

  public static void evaluateNative(Schema inSchema, Tuple input, RowWriter builder, EvalNode[] evals) {
    builder.startRow();
    for (int i = 0; i < evals.length; i++) {
      evals[i].eval(inSchema, input, builder);
    }
    builder.endRow();
  }

  public static void writeEvalResult(RowWriter builder, TajoDataTypes.Type type, Datum datum) {
    switch (type) {
    case NULL_TYPE:
      builder.skipField();
      break;
    case BOOLEAN:
      builder.putBool(datum.asBool());
      break;
    case INT1:
    case INT2:
      builder.putInt2(datum.asInt2());
      break;
    case INT4:
      builder.putInt4(datum.asInt4());
      break;
    case INT8:
      builder.putInt8(datum.asInt8());
      break;
    case FLOAT4:
      builder.putFloat4(datum.asFloat4());
      break;
    case FLOAT8:
      builder.putFloat8(datum.asFloat8());
      break;
    case TIMESTAMP:
      builder.putTimestamp(datum.asInt8());
      break;
    case TIME:
      builder.putTime(datum.asInt8());
      break;
    case DATE:
      builder.putDate(datum.asInt4());
      break;
    case INTERVAL:
      builder.putInterval((org.apache.tajo.datum.IntervalDatum) datum);
      break;
    case CHAR:
    case TEXT:
      builder.putText(datum.asTextBytes());
      break;
    case BLOB:
      builder.putBlob(datum.asByteArray());
      break;
    case INET4:
      builder.putInet4(datum.asInt4());
      break;
    case PROTOBUF:
      builder.putProtoDatum((org.apache.tajo.datum.ProtobufDatum) datum);
      break;
    default:
      throw new UnsupportedException("Unknown Type: " + type.name());
    }
  }
}
