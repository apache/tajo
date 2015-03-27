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

package org.apache.tajo.datum;

import com.google.gson.annotations.Expose;

import static org.apache.tajo.common.TajoDataTypes.Type.ANY;

/**
 * <code>AnyDatum</code> can contain any types of datum.
 */
public class AnyDatum extends Datum {
  @Expose Datum val;

  public AnyDatum(Datum val) {
    super(ANY);
    this.val = val;
  }

  public Datum getActual() {
    return val;
  }

  @Override
  public int size() {
    return this.val.size();
  }

  @Override
  public int hashCode() {
    return val.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AnyDatum) {
      AnyDatum other = (AnyDatum) obj;
      return val.equals(other.val);
    }
    return false;
  }

  @Override
  public Datum equalsTo(Datum datum) {
    if (datum.type() == ANY) {
      AnyDatum other = (AnyDatum) datum;
      return val.equalsTo(other.val);
    }
    return DatumFactory.createBool(false);
  }

  @Override
  public int compareTo(Datum datum) {
    if (datum.type() == ANY) {
      AnyDatum other = (AnyDatum) datum;
      return val.compareTo(other.val);
    }
    // Any datums will be lastly appeared.
    return 1;
  }

  @Override
  public String toString() {
    return val.toString();
  }
}
