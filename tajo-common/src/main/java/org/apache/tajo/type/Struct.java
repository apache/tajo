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

import com.google.common.collect.ImmutableList;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.util.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class Struct extends Type {
  private final ImmutableList<Type> memberTypes;

  public Struct(Collection<Type> memberTypes) {
    this.memberTypes = ImmutableList.copyOf(memberTypes);
  }

  public int size() {
    return memberTypes.size();
  }

  public Type memberType(int idx) {
    return memberTypes.get(idx);
  }

  public List<Type> memberTypes() {
    return this.memberTypes;
  }

  @Override
  public TajoDataTypes.Type baseType() {
    return TajoDataTypes.Type.RECORD;
  }

  @Override
  public String toString() {
    return "struct(" + StringUtils.join(memberTypes, ",") + ")";
  }

  @Override
  public int hashCode() {
    return Objects.hash(baseType(), Objects.hash(memberTypes));
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof Struct) {
      Struct other = (Struct) object;
      return memberTypes.equals(other.memberTypes);
    }

    return false;
  }
}
