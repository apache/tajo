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

package tajo.datum;

import com.google.gson.annotations.Expose;
import tajo.datum.json.GsonCreator;

import static tajo.common.TajoDataTypes.Type;

public class ArrayDatum extends Datum {
  @Expose private Datum [] data;
  public ArrayDatum(Datum [] data) {
    super(Type.ARRAY);
    this.data = data;
  }

  public ArrayDatum(int size) {
    super(Type.ARRAY);
    this.data = new Datum[size];
  }

  public Datum get(int idx) {
    return data[idx];
  }

  public Datum [] toArray() {
    return data;
  }

  public void put(int idx, Datum datum) {
    data[idx] = datum;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public int compareTo(Datum datum) {
    return 0; // TODO - to be implemented
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    boolean first = true;
    for (Datum field : data) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      sb.append(field.asChars());
    }
    sb.append("]");

    return sb.toString();
  }

  @Override
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, Datum.class);
  }
}
