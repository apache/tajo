/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.storage.hcfile.reader;

import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.Datum;
import tajo.storage.exception.UnknownDataTypeException;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class TypeReader {

  public static TypeReader get(DataType type)
      throws IOException, UnknownDataTypeException {

    switch (type) {
      case BOOLEAN:
      case BYTE:
        return new ByteReader();
      case CHAR:
        return new CharReader();
      case BYTES:
        return new BytesReader();
      case SHORT:
        return new ShortReader();
      case INT:
        return new IntReader();
      case LONG:
        return new LongReader();
      case FLOAT:
        return new FloatReader();
      case DOUBLE:
        return new DoubleReader();
      case STRING2:
      case STRING:
        return new StringReader();
      case IPv4:
        return new IPv4Reader();
      case ARRAY:
        return new ArrayReader();
      default:
        throw new UnknownDataTypeException(type.name());
    }
  }

  public abstract Datum read(ByteBuffer buffer) throws IOException;

  public void close() throws IOException {

  }
}
