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
import com.google.common.collect.Maps;
import com.google.gson.annotations.Expose;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.util.ClassUtil;
import org.apache.tajo.util.TUtil;

import java.util.Map;

import static org.apache.tajo.catalog.proto.CatalogProtos.StaticMethodInvocationDescProto;


/**
 * StaticMethodInvokeDesc
 * ClassBaseInvokeDesc
 */
public class StaticMethodInvocationDesc implements ProtoObject<StaticMethodInvocationDescProto> {
  @Expose
  private Class baseClassName;
  @Expose
  private String methodName;
  @Expose
  private Class returnClass;
  @Expose
  private Class [] paramClasses;
  
  public static final Map<String, Class> PRIMITIVE_CLASS_MAP = Maps.newHashMap();
  
  static {
    PRIMITIVE_CLASS_MAP.put("int", Integer.TYPE );
    PRIMITIVE_CLASS_MAP.put("long", Long.TYPE );
    PRIMITIVE_CLASS_MAP.put("double", Double.TYPE );
    PRIMITIVE_CLASS_MAP.put("float", Float.TYPE );
    PRIMITIVE_CLASS_MAP.put("bool", Boolean.TYPE );
    PRIMITIVE_CLASS_MAP.put("char", Character.TYPE );
    PRIMITIVE_CLASS_MAP.put("byte", Byte.TYPE );
    PRIMITIVE_CLASS_MAP.put("void", Void.TYPE );
    PRIMITIVE_CLASS_MAP.put("short", Short.TYPE );

  }

  public StaticMethodInvocationDesc(Class baseClassName, String methodName, Class returnClass, Class[] paramClasses) {
    this.baseClassName = baseClassName;
    this.methodName = methodName;
    this.returnClass = returnClass;
    this.paramClasses = paramClasses;
  }

  public StaticMethodInvocationDesc(StaticMethodInvocationDescProto proto) {
    try {
      baseClassName = Class.forName(proto.getClassName());
      methodName = proto.getMethodName();
      returnClass = ClassUtil.forName(proto.getReturnClass());
      paramClasses = new Class[proto.getParamClassesCount()];
      for (int i = 0; i < proto.getParamClassesCount(); i++) {
        paramClasses[i] = ClassUtil.forName(proto.getParamClasses(i));
      }
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public Class getBaseClassName() {
    return baseClassName;
  }

  public String getMethodName() {
    return methodName;
  }

  public Class getReturnClass() {
    return returnClass;
  }

  public Class [] getParamClasses() {
    return paramClasses;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StaticMethodInvocationDesc) {
      StaticMethodInvocationDesc other = (StaticMethodInvocationDesc) obj;
      return
          baseClassName.equals(other.baseClassName) &&
              methodName.equals(other.methodName) &&
              returnClass.equals(other.returnClass) &&
              TUtil.checkEquals(paramClasses, other.paramClasses);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(baseClassName, methodName, returnClass, Objects.hashCode(paramClasses));
  }

  public String toString() {
    return baseClassName.getCanonicalName() + "::" + methodName;
  } 


  @Override
  public StaticMethodInvocationDescProto getProto() {
    StaticMethodInvocationDescProto.Builder builder = StaticMethodInvocationDescProto.newBuilder();
    builder.setClassName(baseClassName.getName());
    builder.setMethodName(methodName);
    builder.setReturnClass(returnClass.getName());

    for (Class<?> c : paramClasses) {
      builder.addParamClasses(c.getName());
    }
    return builder.build();
  }
}
