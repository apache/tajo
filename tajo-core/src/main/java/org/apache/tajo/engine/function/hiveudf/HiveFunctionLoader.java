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

package org.apache.tajo.engine.function.hiveudf;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.io.*;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.function.*;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

public class HiveFunctionLoader {
  public static Collection<FunctionDesc> loadHiveUDFs(TajoConf conf) {
    String udfdir = conf.get("hive.udf.dir", "lib/hiveudf");
    ArrayList<FunctionDesc> funcList = new ArrayList<>();

    try {
      FileSystem localFS = FileSystem.getLocal(conf);
      Path udfPath = new Path(udfdir);

      if (!localFS.isDirectory(udfPath)) {
        return null;
      }

      for (FileStatus fstatus : localFS.listStatus(udfPath, (Path path) -> path.getName().endsWith(".jar"))) {

        URL[] urls = new URL[]{new URL("jar:" + fstatus.getPath().toUri().toURL() + "!/")};

        // For UDF's decendants (legacy)
        Set<Class<? extends UDF>> udfClasses = getSubclassesFromJarEntry(urls, UDF.class);
        if (udfClasses != null) {
          analyzeUDFclasses(udfClasses, funcList);
        }

        // For GenericUDF's decendants (newer interface)
        Set<Class<? extends GenericUDF>> genericUDFclasses = getSubclassesFromJarEntry(urls, GenericUDF.class);
        if (genericUDFclasses != null) {
          analyzeGenericUDFclasses(genericUDFclasses, funcList);
        }

      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return funcList;
  }

  private static <T> Set<Class<? extends T>> getSubclassesFromJarEntry(URL[] urls, Class<T> targetCls) {
    Reflections refl = new Reflections(new ConfigurationBuilder().
        setUrls(urls).
        addClassLoader(new URLClassLoader(urls)));

    return refl.getSubTypesOf(targetCls);
  }

  static void analyzeUDFclasses(Set<Class<? extends UDF>> classes, List<FunctionDesc> list) {
    for (Class<? extends UDF> clazz: classes) {
      String [] names;
      boolean deterministic = true;

      Description desc = clazz.getAnnotation(Description.class);
      if (desc != null) {
        names = desc.name().split(",");
        for (int i=0; i<names.length; i++) {
          names[i] = names[i].trim();
        }
      }
      else {
        names = new String [] {clazz.getName().replace('.','_')};
      }

      for (String name: names) {
        HiveFunctionRegistry.registerUDF(name, clazz);
      }

      Class hiveUDFretType = null;
      Class [] hiveUDFparams = null;

      for (Method method: clazz.getMethods()) {
        if (method.getName().equals("evaluate")) {
          hiveUDFretType = method.getReturnType();
          hiveUDFparams = method.getParameterTypes();
          break;
        }
      }

      TajoDataTypes.DataType retType = convertHiveTypeToTajoType(hiveUDFretType);
      TajoDataTypes.DataType [] params = null;

      if (hiveUDFparams != null && hiveUDFparams.length > 0) {
        params = new TajoDataTypes.DataType[hiveUDFparams.length];
        for (int i=0; i<hiveUDFparams.length; i++) {
          params[i] = convertHiveTypeToTajoType(hiveUDFparams[i]);
        }
      }

      UDFType type = clazz.getDeclaredAnnotation(UDFType.class);
      if (type != null) {
        deterministic = type.deterministic();
      }

      FunctionSignature signature = new FunctionSignature(CatalogProtos.FunctionType.UDF, names[0], retType, deterministic, params);
      FunctionInvocation invocation = new FunctionInvocation();
      invocation.setLegacy(new ClassBaseInvocationDesc<>(HiveGeneralFunctionHolder.class));
      FunctionSupplement supplement = new FunctionSupplement();
      
      FunctionDesc funcDesc = new FunctionDesc(signature, invocation, supplement);

      list.add(funcDesc);
    }
  }

  private static void analyzeGenericUDFclasses(Set<Class<? extends GenericUDF>> classes, ArrayList<FunctionDesc> list) {
  }

  private static TajoDataTypes.DataType convertHiveTypeToTajoType(Class hiveType) {
    if (hiveType == null)
      return null;

    if (hiveType == IntWritable.class) {
      return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT4);
    }
    if (hiveType == LongWritable.class) {
      return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT8);
    }
    if (hiveType == Text.class) {
      return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.TEXT);
    }
    if (hiveType == FloatWritable.class) {
      return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT4);
    }
    if (hiveType == DoubleWritable.class) {
      return CatalogUtil.newSimpleDataType(TajoDataTypes.Type.FLOAT8);
    }

    return null;
  }
}
