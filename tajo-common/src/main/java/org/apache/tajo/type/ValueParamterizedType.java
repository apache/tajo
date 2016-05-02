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
import java.util.Objects;

/**
 * Represents a type which takes value parameters (e.g., Numeric(10, 6))
 */
public abstract class ValueParamterizedType extends Type {
  protected ImmutableList<Object> params;

  public ValueParamterizedType(TajoDataTypes.Type type, ImmutableList<Object> params) {
    super(type);
    this.params = params;
  }

  @Override
  public boolean isValueParameterized() {
    return true;
  }

  @Override
  public Collection<Object> getValueParameters() {
    return params;
  }

  @Override
  public boolean equals(Object object) {
    if (object == this) {
      return true;
    }

    if (object instanceof ValueParamterizedType) {
      ValueParamterizedType other = (ValueParamterizedType) object;
      return this.baseType.equals(other.baseType) && params.equals(other.params);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(baseType(), params);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(typeName(this.baseType));
    sb.append("(");
    sb.append(StringUtils.join(params, ","));
    sb.append(")");
    return sb.toString();
  }
}
