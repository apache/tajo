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
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.tajo.util.TUtil;

public class DataTypeExpr extends Expr {
  @Expose @SerializedName("DataTypeName")
  String typeName;
  @Expose @SerializedName("LengthOrPrecision")
  Integer lengthOrPrecision;
  @Expose @SerializedName("Scale")
  Integer scale;

  public DataTypeExpr(String typeName) {
    super(OpType.DataType);
    this.typeName = typeName;
  }

  public String getTypeName() {
    return this.typeName;
  }

  public boolean hasLengthOrPrecision() {
    return lengthOrPrecision != null;
  }

  public void setLengthOrPrecision(int lengthOrPrecision) {
    this.lengthOrPrecision = lengthOrPrecision;
  }

  public Integer getLengthOrPrecision() {
    return this.lengthOrPrecision;
  }

  public boolean hasScale() {
    return this.scale != null;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }

  public Integer getScale() {
    return this.scale;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(typeName, lengthOrPrecision, scale);
  }

  @Override
  boolean equalsTo(Expr expr) {
    DataTypeExpr another = (DataTypeExpr) expr;
    return typeName.equals(another.typeName) &&
        TUtil.checkEquals(lengthOrPrecision, another.lengthOrPrecision) &&
        TUtil.checkEquals(scale, another.scale);
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    DataTypeExpr dataType = (DataTypeExpr) super.clone();
    dataType.typeName = typeName;
    dataType.lengthOrPrecision = lengthOrPrecision;
    dataType.scale = scale;
    return dataType;
  }
}
