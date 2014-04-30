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

package org.apache.tajo.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public abstract class ClassUtil {
  private static final Log LOG = LogFactory.getLog(ClassUtil.class);

  public static Set<Class> findClasses(Class type, String packageFilter) {
    Set<Class> classSet = new HashSet<Class>();

    String classpath = System.getProperty("java.class.path");
    String[] paths = classpath.split(System.getProperty("path.separator"));

    for (String path : paths) {
      File file = new File(path);
      if (file.exists()) {
        findClasses(classSet, file, file, true, type, packageFilter);
      }
    }

    return classSet;
  }

  private static void findClasses(Set<Class> matchedClassSet, File root, File file, boolean includeJars, Class type,
                                  String packageFilter) {
    if (file.isDirectory()) {
      for (File child : file.listFiles()) {
        findClasses(matchedClassSet, root, child, includeJars, type, packageFilter);
      }
    } else {
      if (file.getName().toLowerCase().endsWith(".jar") && includeJars) {
        JarFile jar = null;
        try {
          jar = new JarFile(file);
        } catch (Exception ex) {
          LOG.error(ex.getMessage(), ex);
          return;
        }
        Enumeration<JarEntry> entries = jar.entries();
        while (entries.hasMoreElements()) {
          JarEntry entry = entries.nextElement();
          String name = entry.getName();
          int extIndex = name.lastIndexOf(".class");
          if (extIndex > 0) {
            String qualifiedClassName = name.substring(0, extIndex).replace("/", ".");
            if (qualifiedClassName.indexOf(packageFilter) >= 0 && !isTestClass(qualifiedClassName)) {
              try {
                Class clazz = Class.forName(qualifiedClassName);

                if (!clazz.isInterface() && isMatch(type, clazz)) {
                  matchedClassSet.add(clazz);
                }
              } catch (ClassNotFoundException e) {
                LOG.error(e.getMessage(), e);
              }
            }
          }
        }
      } else if (file.getName().toLowerCase().endsWith(".class")) {
        String qualifiedClassName = createClassName(root, file);
        if (qualifiedClassName.indexOf(packageFilter) >= 0 && !isTestClass(qualifiedClassName)) {
          try {
            Class clazz = Class.forName(qualifiedClassName);
            if (!clazz.isInterface() && isMatch(type, clazz)) {
              matchedClassSet.add(clazz);
            }
          } catch (ClassNotFoundException e) {
            LOG.error(e.getMessage(), e);
          }
        }
      }
    }
  }

  private static boolean isTestClass(String qualifiedClassName) {
    String className = getSimpleClassName(qualifiedClassName);
    if(className == null) {
      return false;
    }

    return className.startsWith("Test");
  }

  private static boolean isMatch(Class targetClass, Class loadedClass) {
    if (targetClass.equals(loadedClass)) {
      return true;
    }

    Class[] classInterfaces = loadedClass.getInterfaces();
    if (classInterfaces != null) {
      for (Class eachInterfaceClass : classInterfaces) {
        if (eachInterfaceClass.equals(targetClass)) {
          return true;
        }

        if (isMatch(targetClass, eachInterfaceClass)) {
          return true;
        }
      }
    }

    Class superClass = loadedClass.getSuperclass();
    if (superClass != null) {
      if (isMatch(targetClass, superClass)) {
        return true;
      }
    }
    return false;
  }

  private static String getSimpleClassName(String qualifiedClassName) {
    String[] tokens = qualifiedClassName.split("\\.");
    if (tokens.length == 0) {
      return qualifiedClassName;
    }
    return tokens[tokens.length - 1];
  }

  private static String createClassName(File root, File file) {
    StringBuffer sb = new StringBuffer();
    String fileName = file.getName();
    sb.append(fileName.substring(0, fileName.lastIndexOf(".class")));
    file = file.getParentFile();
    while (file != null && !file.equals(root)) {
      sb.insert(0, '.').insert(0, file.getName());
      file = file.getParentFile();
    }
    return sb.toString();
  }
}
