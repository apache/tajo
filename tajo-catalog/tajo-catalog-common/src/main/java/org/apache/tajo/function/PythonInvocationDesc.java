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

package org.apache.tajo.function;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.proto.CatalogProtos.PythonInvocationDescProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.util.TUtil;

/**
 * <code>PythonInvocationDesc</code> describes a function name
 * and a file path to the script where the function is defined.
 */
public class PythonInvocationDesc implements ProtoObject<PythonInvocationDescProto>, Cloneable {
  @Expose private String funcName;
  @Expose private String filePath;

  public PythonInvocationDesc() {

  }

  public PythonInvocationDesc(String funcName, String filePath) {
    this.funcName = funcName;
    this.filePath = filePath;
  }

  public void setFuncName(String funcName) {
    this.funcName = funcName;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public PythonInvocationDesc(PythonInvocationDescProto proto) {
    this(proto.getFuncName(), proto.getFilePath());
  }

  public String getName() {
    return funcName;
  }

  public String getPath() {
    return filePath;
  }

  @Override
  public PythonInvocationDescProto getProto() {
    PythonInvocationDescProto.Builder builder = PythonInvocationDescProto.newBuilder();
    builder.setFuncName(funcName).setFilePath(filePath);
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PythonInvocationDesc) {
      PythonInvocationDesc other = (PythonInvocationDesc) o;
      return TUtil.checkEquals(funcName, other.funcName) &&
          TUtil.checkEquals(filePath, other.filePath);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(funcName, filePath);
  }

  @Override
  public String toString() {
    return funcName + " at " + filePath;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    PythonInvocationDesc clone = (PythonInvocationDesc) super.clone();
    clone.funcName = funcName == null ? null : funcName;
    clone.filePath = filePath == null ? null : filePath;
    return clone;
  }
}
