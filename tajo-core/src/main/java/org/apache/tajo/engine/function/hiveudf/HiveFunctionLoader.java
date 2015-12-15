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
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.FunctionDescBuilder;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.function.UDFInvocationDesc;
import org.apache.tajo.util.WritableTypeConverter;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

public class HiveFunctionLoader {
  public static Collection<FunctionDesc> loadHiveUDFs(TajoConf conf) {
    String udfdir = conf.get(TajoConf.ConfVars.HIVE_UDF_DIR.varname);
    ArrayList<FunctionDesc> funcList = new ArrayList<>();

    try {
      FileSystem localFS = FileSystem.getLocal(conf);
      Path udfPath = new Path(udfdir);

      if (!localFS.isDirectory(udfPath)) {
        return null;
      }

      // loop each jar file
      for (FileStatus fstatus : localFS.listStatus(udfPath, (Path path) -> path.getName().endsWith(".jar"))) {

        URL[] urls = new URL[]{new URL("jar:" + fstatus.getPath().toUri().toURL() + "!/")};

        // extract and register UDF's decendants (legacy Hive UDF form)
        Set<Class<? extends UDF>> udfClasses = getSubclassesFromJarEntry(urls, UDF.class);
        if (udfClasses != null) {
          buildFunctionsFromUDF(udfClasses, funcList, "jar:"+urls[0].getPath());
        }
      }
    } catch (IOException e) {
      throw new TajoInternalError(e);
    }

    return funcList;
  }

  private static <T> Set<Class<? extends T>> getSubclassesFromJarEntry(URL[] urls, Class<T> targetCls) {
    Reflections refl = new Reflections(new ConfigurationBuilder().
        setUrls(urls).
        addClassLoader(new URLClassLoader(urls)));

    return refl.getSubTypesOf(targetCls);
  }

  static void buildFunctionsFromUDF(Set<Class<? extends UDF>> classes, List<FunctionDesc> list, String jarurl) {
    for (Class<? extends UDF> clazz: classes) {
      String [] names;
      String value = null, extended = null;

      Description desc = clazz.getAnnotation(Description.class);

      // Check @Description annotation (if exists)
      if (desc != null) {
        names = desc.name().split(",");
        for (int i=0; i<names.length; i++) {
          names[i] = names[i].trim();
        }

        value = desc.value();
        extended = desc.extended();
      }
      else {
        names = new String [] {clazz.getName().replace('.','_')};
      }

      Class hiveUDFretType = null;
      Class [] hiveUDFparams = null;

      // verify 'evaluate' method and extract return type and parameter types
      for (Method method: clazz.getMethods()) {
        if (method.getName().equals("evaluate")) {
          hiveUDFretType = method.getReturnType();
          hiveUDFparams = method.getParameterTypes();
          break;
        }
      }

      TajoDataTypes.DataType retType = WritableTypeConverter.convertWritableToTajoType(hiveUDFretType);
      TajoDataTypes.DataType [] params = null;

      // convert types to ones of Tajo
      if (hiveUDFparams != null && hiveUDFparams.length > 0) {
        params = new TajoDataTypes.DataType[hiveUDFparams.length];
        for (int i=0; i<hiveUDFparams.length; i++) {
          params[i] = WritableTypeConverter.convertWritableToTajoType(hiveUDFparams[i]);
        }
      }

      // actual function descriptor building
      FunctionDescBuilder builder = new FunctionDescBuilder();

      UDFType type = clazz.getDeclaredAnnotation(UDFType.class);
      if (type != null) {
        builder.setDeterministic(type.deterministic());
      }

      builder.setFunctionType(CatalogProtos.FunctionType.UDF).setReturnType(retType).setParams(params);

      if (value != null) {
        builder.setDescription(value);
      }

      if (extended != null) {
        builder.setExample(extended);
      }

      UDFInvocationDesc udfInvocation = new UDFInvocationDesc(CatalogProtos.UDFtype.HIVE, clazz.getName(), jarurl, true);

      for (String name: names) {
        builder.setName(name);
        builder.setUDF(udfInvocation);
        list.add(builder.build());
      }
    }
  }
}
