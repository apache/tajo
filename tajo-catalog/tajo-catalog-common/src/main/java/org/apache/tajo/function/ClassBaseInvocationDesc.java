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

import com.google.gson.annotations.Expose;
import org.apache.tajo.common.ProtoObject;

import static org.apache.tajo.catalog.proto.CatalogProtos.ClassBaseInvocationDescProto;

public class ClassBaseInvocationDesc<T> implements ProtoObject<ClassBaseInvocationDescProto> {
  @Expose
  private Class<? extends T> functionClass;

  public ClassBaseInvocationDesc(Class<? extends T> functionClass) {
    this.functionClass = functionClass;
  }

  public ClassBaseInvocationDesc(ClassBaseInvocationDescProto proto) {
    try {
      this.functionClass = (Class<? extends T>) Class.forName(proto.getClassName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public Class<? extends T> getFunctionClass() {
    return functionClass;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ClassBaseInvocationDesc) {
      ClassBaseInvocationDesc other = (ClassBaseInvocationDesc) obj;
      return functionClass.equals(other.functionClass);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return functionClass.getCanonicalName().hashCode();
  }

  public String toString() {
    return functionClass.getCanonicalName();
  }

  @Override
  public ClassBaseInvocationDescProto getProto() {
    ClassBaseInvocationDescProto.Builder builder = ClassBaseInvocationDescProto.newBuilder();
    builder.setClassName(functionClass.getName());
    return builder.build();
  }
}
