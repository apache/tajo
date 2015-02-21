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

package org.apache.tajo.datum;

import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.InvalidOperationException;

public class StructDatum extends Datum {
  private Datum [] values;

  public StructDatum(Datum [] values) {
    super(Type.STRUCT);
    this.values = values;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum instanceof StructDatum) {
      StructDatum other = (StructDatum) datum;
      int min = Math.min(values.length, other.values.length);

      for (int i = 0; i < min; i++) {
        int compVal = values[i].compareTo(other.values[i]);
        if (compVal != 0) {
          return compVal;
        }
      }

      // the narrow width is regarded as lower one.
      return values.length - other.values.length;

    } else {
      throw new InvalidOperationException(datum.type());
    }
  }
}
