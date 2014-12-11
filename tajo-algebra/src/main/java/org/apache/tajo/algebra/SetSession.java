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

package org.apache.tajo.algebra;


import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class SetSession extends Expr {
  @Expose @SerializedName("name")
  private String name;
  @Expose @SerializedName("value")
  private String value;

  public SetSession(String name, String value) {
    super(OpType.SetSession);

    this.name = name;
    this.value = value;
  }

  public boolean isDefault() {
    return value == null;
  }

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }

  public int hashCode() {
    return Objects.hashCode(name, value);
  }

  boolean equalsTo(Expr expr) {
    SetSession another = (SetSession) expr;
    return name.equals(another.name) && value.equals(another.value);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    SetSession setOperation = (SetSession) super.clone();
    setOperation.name = name;
    setOperation.value = value;
    return setOperation;
  }
}
