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

package org.apache.tajo.type;

import org.apache.tajo.common.TajoDataTypes;

import java.util.Objects;

public class Char extends Type {
  private final int length;

  public Char(int length) {
    this.length = length;
  }

  public int length() {
    return length;
  }

  @Override
  public boolean hasParam() {
    return true;
  }

  @Override
  public TajoDataTypes.Type baseType() {
    return TajoDataTypes.Type.CHAR;
  }

  @Override
  public String toString() {
    return "char(" + length + ")";
  }

  @Override
  public int hashCode() {
    return Objects.hash(baseType(), length);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof Char) {
      return length == ((Char) object).length;
    }

    return false;
  }
}
