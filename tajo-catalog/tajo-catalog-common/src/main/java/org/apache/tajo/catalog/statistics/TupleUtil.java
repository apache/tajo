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

package org.apache.tajo.catalog.statistics;

import com.google.protobuf.Message;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.*;

import java.io.IOException;

public class TupleUtil {
  public static Datum createFromBytes(DataType type, byte [] bytes) {
    switch (type.getType()) {
      case BOOLEAN:
        return new BooleanDatum(bytes);
      case BLOB:
        return new BlobDatum(bytes);
      case CHAR:
        return new CharDatum(bytes);
      case INT2:
        return new Int2Datum(bytes);
      case INT4:
        return new Int4Datum(bytes);
      case INT8:
        return new Int8Datum(bytes);
      case FLOAT4:
        return new Float4Datum(bytes);
      case FLOAT8:
        return new Float8Datum(bytes);
      case TEXT:
        return new TextDatum(bytes);
      case INET4:
        return new Inet4Datum(bytes);
      case PROTOBUF:
        ProtobufDatumFactory factory = ProtobufDatumFactory.get(type);
        Message.Builder builder = factory.newBuilder();
        try {
          builder.mergeFrom(bytes);
          return factory.createDatum(builder.build());
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      default: throw new UnsupportedOperationException(type + " is not supported yet");
    }
  }
}
