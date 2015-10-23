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
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.tajo.conf.TajoConf;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class HiveFunctionLoader {
  public static void loadHiveUDFs(TajoConf conf) {
    String udfdir = conf.get("hive.udf.dir", "lib/hiveudf");
    //Reflections refl = new Reflections(new ConfigurationBuilder().setUrls());

    try {
      FileSystem localFS = FileSystem.getLocal(conf);
      Path udfPath = new Path(udfdir);

      if (!localFS.isDirectory(udfPath)) {
        return;
      }

      for (FileStatus fstatus: localFS.listStatus(udfPath, (Path path)->path.getName().endsWith(".jar"))) {

        String absPath = fstatus.getPath().toString();

        // Eliminate "file:"
        if (absPath.startsWith("file:")) {
          absPath = absPath.substring(5);
        }

        JarFile jar = new JarFile(absPath);
        Enumeration<JarEntry> e = jar.entries();
        URL[] urls = getURLs(fstatus);

        while (e.hasMoreElements()) {
          Set<Class<? extends UDF>> UDFclasses = getClassesFromJarEntry(e.nextElement(), urls, UDF.class);
          Set<Class<? extends GenericUDF>> GenericUDFclasses = getClassesFromJarEntry(e.nextElement(), urls, GenericUDF.class);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static URL [] getURLs(FileStatus fstatus) throws MalformedURLException {
    URL fileURL = fstatus.getPath().toUri().toURL();
    String jarURL = "jar:" + fileURL + "!/";

    return new URL [] { new URL(jarURL) };
  }

  private static <T> Set<Class<? extends T>> getClassesFromJarEntry(JarEntry entry, URL [] urls, Class<T> targetCls) {
    String name = entry.getName();
    if (entry.isDirectory() || !name.endsWith(".class")) {
      return null;
    }

    Reflections refl = new Reflections(new ConfigurationBuilder().
        setUrls(urls).
        addClassLoader(new URLClassLoader(urls)));

    return refl.getSubTypesOf(targetCls);
  }
}
