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

package org.apache.tajo.plan.function;

import org.apache.hadoop.io.*;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.exception.*;
import org.apache.tajo.function.UDFInvocationDesc;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.plan.util.WritableTypeConverter;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

public class HiveFunctionInvoke extends FunctionInvoke implements Cloneable {
  private Object instance = null;
  private Method evalMethod = null;
  private Writable [] params;

  public HiveFunctionInvoke(FunctionDesc desc) {
    super(desc);
    params = new Writable[desc.getParamTypes().length];
  }

  @Override
  public void init(FunctionInvokeContext context) throws IOException {
    UDFInvocationDesc udfDesc = functionDesc.getInvocation().getUDF();

    URL [] urls = new URL [] { new URL(udfDesc.getPath()) };
    URLClassLoader loader = new URLClassLoader(urls);

    try {
      Class<?> udfclass = loader.loadClass(udfDesc.getName());
      evalMethod = getEvaluateMethod(functionDesc.getParamTypes(), udfclass);
    } catch (ClassNotFoundException e) {
      throw new TajoInternalError(e);
    }
  }

  private Method getEvaluateMethod(TajoDataTypes.DataType [] tajoParamTypes, Class<?> clazz) {
    Constructor constructor = clazz.getConstructors()[0];

    try {
      instance = constructor.newInstance();
    } catch (InstantiationException|IllegalAccessException|InvocationTargetException e) {
      throw new TajoInternalError(e);
    }

    for (Method m: clazz.getMethods()) {
      if (m.getName().equals("evaluate")) {
        Class [] methodParamTypes = m.getParameterTypes();
        if (checkParamTypes(methodParamTypes, tajoParamTypes)) {
          return m;
        }
      }
    }
    
    throw new TajoInternalError(new UndefinedFunctionException(String.format("Hive UDF (%s)", clazz.getSimpleName())));
  }

  private boolean checkParamTypes(Class [] writableParams, TajoDataTypes.DataType [] tajoParams) {
    int i = 0;

    if (writableParams.length != tajoParams.length) {
      return false;
    }

    for (Class writable: writableParams) {
      try {
        if (!WritableTypeConverter.convertWritableToTajoType(writable).equals(tajoParams[i++])) {
          return false;
        }
      } catch (UnsupportedDataTypeException e) {
        throw new TajoRuntimeException(e);
      }
    }

    return true;
  }

  @Override
  public Datum eval(Tuple tuple) {
    Datum resultDatum;

    for (int i=0; i<tuple.size(); i++) {
      params[i] = WritableTypeConverter.convertDatum2Writable(tuple.asDatum(i));
    }

    try {
      Writable result = (Writable)evalMethod.invoke(instance, params);
      resultDatum = WritableTypeConverter.convertWritable2Datum(result);
    } catch (Exception e) {
      throw new TajoInternalError(e);
    }

    return resultDatum;
  }
}
