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

package org.apache.tajo.engine.function;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.function.FunctionInvocation;
import org.apache.tajo.function.FunctionSignature;
import org.apache.tajo.function.FunctionSupplement;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class HiveFunctionLoader {
  public static void loadHiveUDFs(TajoConf conf) {
    String udfdir = conf.get("hive.udf.dir", "lib/hiveudf");

    try {
      FileSystem localFS = FileSystem.getLocal(conf);
      Path udfPath = new Path(udfdir);

      if (!localFS.isDirectory(udfPath)) {
        return;
      }

      for (FileStatus fstatus : localFS.listStatus(udfPath, (Path path) -> path.getName().endsWith(".jar"))) {

        URL[] urls = new URL[]{new URL("jar:" + fstatus.getPath().toUri().toURL() + "!/")};

        List<FunctionDesc> udfList = new LinkedList<>();

        // For UDF's decendants (legacy)
        Set<Class<? extends UDF>> udfClasses = getSubclassesFromJarEntry(urls, UDF.class);
        if (udfClasses != null) {
          analyzeUDFclasses(udfClasses, udfList);
        }

        // For GenericUDF's decendants (newer interface)
        Set<Class<? extends GenericUDF>> genericUDFclasses = getSubclassesFromJarEntry(urls, GenericUDF.class);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static <T> Set<Class<? extends T>> getSubclassesFromJarEntry(URL[] urls, Class<T> targetCls) {
    Reflections refl = new Reflections(new ConfigurationBuilder().
        setUrls(urls).
        addClassLoader(new URLClassLoader(urls)));

    return refl.getSubTypesOf(targetCls);
  }

  private static void analyzeUDFclasses(Set<Class<? extends UDF>> classes, List<FunctionDesc> list) {
    for (Class<? extends UDF> clazz: classes) {
      String name;
      FunctionSignature signature;
      FunctionInvocation invocation = new FunctionInvocation();
      FunctionSupplement supplement = new FunctionSupplement();

      Description desc = clazz.getDeclaredAnnotation(Description.class);

      name = desc == null ? clazz.getSimpleName() : desc.name();

      System.out.println(name);
    }
  }
}
