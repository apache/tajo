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

package org.apache.tajo.catalog;

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.type.*;

public class TypeConverter {
  public static Type convert(TajoDataTypes.DataType legacyType) {
    switch (legacyType.getType()) {
    case BOOLEAN:
      return new Bool();
    case INT1:
    case INT2:
      return new Int2();
    case INT4:
      return new Int4();
    case INT8:
      return new Int8();
    case FLOAT4:
      return new Float4();
    case FLOAT8:
      return new Float8();
    case DATE:
      return new Date();
    case TIME:
      return new Time();
    case TIMESTAMP:
      return new Date();
    case TEXT:
      return new Text();
    case BLOB:
      return new Blob();
    default:
      throw new TajoRuntimeException(new UnsupportedException(legacyType.getType().name()));
    }
  }
}
