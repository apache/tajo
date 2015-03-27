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

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.proto.CatalogProtos.PythonInvocationDescProto;
import org.apache.tajo.common.ProtoObject;

public class PythonInvocationDesc implements ProtoObject<PythonInvocationDescProto> {
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
}
