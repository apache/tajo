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

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

/**
 * KeyTuple is to keep its hash value in memory to avoid frequent expensive hash calculation.
 * Datum.hashCode() uses MurmurHash, so its cost is not so cheap.
 *
 */
public class KeyTuple extends VTuple implements Cloneable {
  private int hashCode;

  public KeyTuple(int size) {
    super(size);
    updateHashCode();
  }

  public KeyTuple(Tuple tuple) {
    super(tuple);
    updateHashCode();
  }

  public KeyTuple(Datum[] datums) {
    super(datums);
    updateHashCode();
  }

  @Override
  public void insertTuple(int fieldId, Tuple tuple) {
    super.insertTuple(fieldId, tuple);
    updateHashCode();
  }

  private void updateHashCode() {
    this.hashCode = super.hashCode();
  }

  @Override
  public void put(int fieldId, Datum value) {
    super.put(fieldId, value);
    updateHashCode();
  }

  @Override
  public void clear() {
    super.clear();
    updateHashCode();
  }

  @Override
  public void put(Datum [] values) {
    super.put(values);
    updateHashCode();
  }

  @Override
  public KeyTuple clone() throws CloneNotSupportedException {
    return (KeyTuple) super.clone();
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }
}
