/***
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

package org.apache.tajo.function;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.proto.CatalogProtos.PythonAggInvocationDescProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.util.TUtil;

public class PythonAggInvocationDesc implements ProtoObject<PythonAggInvocationDescProto>, Cloneable {
  @Expose private String className;
  @Expose private String filePath;

  public PythonAggInvocationDesc(String className, String filePath) {
    this.className = className;
    this.filePath = filePath;
  }

  public PythonAggInvocationDesc(PythonAggInvocationDescProto proto) {
    this(proto.getClassName(), proto.getFilePath());
  }

  @Override
  public PythonAggInvocationDescProto getProto() {
    PythonAggInvocationDescProto.Builder builder = PythonAggInvocationDescProto.newBuilder();
    builder.setClassName(className).setFilePath(filePath);
    return builder.build();
  }

  public String getClassName() {
    return className;
  }

  public String getFilePath() {
    return filePath;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PythonAggInvocationDesc) {
      PythonAggInvocationDesc other = (PythonAggInvocationDesc) o;
      return TUtil.checkEquals(className, other.className) &&
          TUtil.checkEquals(filePath, other.filePath);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(className, filePath);
  }

  @Override
  public String toString() {
    return className + " at " + filePath;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    PythonAggInvocationDesc clone = (PythonAggInvocationDesc) super.clone();
    clone.className = className;
    clone.filePath = filePath;
    return clone;
  }
}
