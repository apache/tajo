/*
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

package org.apache.tajo.tuple.memory;

import com.google.common.collect.Lists;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.RowBlockReader;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class OffHeapRowBlockUtils {

  public static List<Tuple> sort(MemoryRowBlock rowBlock, Comparator<Tuple> comparator) {
    List<Tuple> tupleList = Lists.newArrayList();

    ZeroCopyTuple zcTuple;
    if(rowBlock.getMemory().hasAddress()) {
      zcTuple = new UnSafeTuple();
    } else {
      zcTuple = new HeapTuple();
    }

    RowBlockReader reader = rowBlock.getReader();
    while(reader.next(zcTuple)) {
      tupleList.add(zcTuple);

      if(rowBlock.getMemory().hasAddress()) {
        zcTuple = new UnSafeTuple();
      } else {
        zcTuple = new HeapTuple();
      }
    }
    Collections.sort(tupleList, comparator);
    return tupleList;
  }

  public static Tuple[] sortToArray(MemoryRowBlock rowBlock, Comparator<Tuple> comparator) {
    Tuple[] tuples = new Tuple[rowBlock.rows()];

    ZeroCopyTuple zcTuple;
    if(rowBlock.getMemory().hasAddress()) {
      zcTuple = new UnSafeTuple();
    } else {
      zcTuple = new HeapTuple();
    }

    RowBlockReader reader = rowBlock.getReader();
    for (int i = 0; i < rowBlock.rows() && reader.next(zcTuple); i++) {
      tuples[i] = zcTuple;
      if(rowBlock.getMemory().hasAddress()) {
        zcTuple = new UnSafeTuple();
      } else {
        zcTuple = new HeapTuple();
      }
    }
    Arrays.sort(tuples, comparator);
    return tuples;
  }

  public static void convert(Tuple tuple, RowWriter writer) {
    writer.startRow();

    for (int i = 0; i < writer.dataTypes().length; i++) {
      if (tuple.isBlankOrNull(i)) {
        writer.skipField();
        continue;
      }
      switch (writer.dataTypes()[i].getType()) {
      case BOOLEAN:
        writer.putBool(tuple.getBool(i));
        break;
      case BIT:
        writer.putByte(tuple.getByte(i));
        break;
      case INT1:
      case INT2:
        writer.putInt2(tuple.getInt2(i));
        break;
      case INT4:
      case DATE:
      case INET4:
        writer.putInt4(tuple.getInt4(i));
        break;
      case INT8:
      case TIMESTAMP:
      case TIME:
        writer.putInt8(tuple.getInt8(i));
        break;
      case FLOAT4:
        writer.putFloat4(tuple.getFloat4(i));
        break;
      case FLOAT8:
        writer.putFloat8(tuple.getFloat8(i));
        break;
      case CHAR:
      case TEXT:
        writer.putText(tuple.getBytes(i));
        break;
      case BLOB:
        writer.putBlob(tuple.getBytes(i));
        break;
      case INTERVAL:
        writer.putInterval((IntervalDatum) tuple.getInterval(i));
        break;
      case PROTOBUF:
        writer.putProtoDatum((ProtobufDatum) tuple.getProtobufDatum(i));
        break;
      case NULL_TYPE:
        writer.skipField();
        break;
      default:
        throw new TajoRuntimeException(
            new UnsupportedException("unknown data type '" + writer.dataTypes()[i].getType().name() + "'"));
      }
    }
    writer.endRow();
  }
}
