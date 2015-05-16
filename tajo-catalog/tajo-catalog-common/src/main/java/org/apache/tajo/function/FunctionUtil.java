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

import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;

import java.util.Collection;
import java.util.Comparator;

import static org.apache.tajo.common.TajoDataTypes.DataType;

public class FunctionUtil {

  public static class FunctionDescProtoComparator implements Comparator<CatalogProtos.FunctionDescProto>
  {
    @Override
    public int compare(CatalogProtos.FunctionDescProto f1, CatalogProtos.FunctionDescProto f2) {
      CatalogProtos.FunctionSignatureProto s1 = f1.getSignature();
      CatalogProtos.FunctionSignatureProto s2 = f2.getSignature();

      int cmpVal = s1.getName().compareTo(s2.getName());

      if (cmpVal != 0) {
        return cmpVal;
      }

      cmpVal = s1.getType().name().compareTo(s2.getType().name());

      if (cmpVal != 0) {
        return cmpVal;
      }

      cmpVal = s1.getReturnType().getType().name().compareTo(s2.getReturnType().getType().name());

      if (cmpVal != 0) {
        return cmpVal;
      }

      for (int i = 0; i < Math.min(s1.getParameterTypesCount(), s2.getParameterTypesCount()); i++) {
        cmpVal = s1.getParameterTypes(i).getType().name().compareTo(s2.getParameterTypes(i).getType().name());

        if (cmpVal != 0) {
          return cmpVal;
        }
      }

      return s2.getParameterTypesCount() - s1.getParameterTypesCount();
    }
  }

  public static String buildFQFunctionSignature(String funcName, DataType returnType, DataType... paramTypes) {
    return returnType.getType().name().toLowerCase() + " " + buildSimpleFunctionSignature(funcName, paramTypes);
  }

  public static String buildSimpleFunctionSignature(String functionName, Collection<DataType> paramTypes) {
    DataType [] types = paramTypes.toArray(new DataType[paramTypes.size()]);
    return buildSimpleFunctionSignature(functionName, types);
  }

  public static String buildSimpleFunctionSignature(String signature, DataType... paramTypes) {
    return signature + "(" + buildParamTypeString(paramTypes) + ")";
  }

  public static String buildParamTypeString(DataType [] paramTypes) {
    StringBuilder sb = new StringBuilder();
    int i = 0;
    for (DataType type : paramTypes) {
      sb.append(type.getType().name().toLowerCase());
      if(i < paramTypes.length - 1) {
        sb.append(",");
      }
      i++;
    }
    return sb.toString();
  }

  public static boolean isNullableParam(Class<?> clazz) {
    return !clazz.isPrimitive();
  }

  public static boolean isLegacyFunction(FunctionDesc desc) {
    return desc.getInvocation().hasLegacy();
  }

  public static boolean isScriptFunction(FunctionDesc desc) {
    return desc.getInvocation().hasPython();
  }
}
