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

package org.apache.tajo.plan.function.python;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.function.FunctionInvocation;
import org.apache.tajo.function.FunctionSignature;
import org.apache.tajo.function.FunctionSupplement;
import org.apache.tajo.function.PythonInvocationDesc;
import org.apache.tajo.plan.function.OptionalFunctionContext;
import org.apache.tajo.util.TUtil;
import org.python.core.*;
import org.python.modules.zipimport.zipimporter;
import org.python.util.PythonInterpreter;

import javax.script.ScriptEngine;

/**
 * Implementation of the script engine for Jython
 */
public class JythonScriptEngine extends TajoScriptEngine {
  private static final Log LOG = LogFactory.getLog(JythonScriptEngine.class);
  public static final String NAMESPACE_SEPARATOR = ".";

  /**
   * Language Interpreter Uses static holder pattern
   */
  private static class Interpreter {
    static final PythonInterpreter interpreter;
    static final ArrayList<String> filesLoaded = new ArrayList<String>();
    static final String JVM_JAR;

    static {
      // should look like: file:JVM_JAR!/java/lang/Object.class
      String rpath = Object.class.getResource("Object.class").getPath();
      JVM_JAR = rpath.replaceAll("^file:(.*)!/java/lang/Object.class$", "$1");

      // Determine if a usable python.cachedir has been provided
      // if not, certain uses of jython's import will not work e.g., so create a tmp dir
      //  - from some.package import *
      //  - import non.jvm.package
      try {
        String skip = System.getProperty(PySystemState.PYTHON_CACHEDIR_SKIP, "false");
        if (skip.equalsIgnoreCase("true")) {
          LOG.warn("jython cachedir skipped, jython may not work");
        } else {
          File tmp = null;
          String cdir = System.getProperty(PySystemState.PYTHON_CACHEDIR);
          if (cdir != null) {
            tmp = new File(cdir);
            if (tmp.canWrite() == false) {
              LOG.error("CACHEDIR: not writable");
              throw new RuntimeException("python.cachedir not writable: " + cdir);
            }
          }
          if (tmp == null) {
            tmp = File.createTempFile("pig_jython_", "");
            tmp.delete();
            if (tmp.mkdirs() == false) {
              LOG.warn("unable to create a tmp dir for the cache, jython may not work");
            } else {
              LOG.info("created tmp python.cachedir=" + tmp);
              System.setProperty(PySystemState.PYTHON_CACHEDIR, tmp.getAbsolutePath());
            }
            Runtime.getRuntime().addShutdownHook(new DirDeleter(tmp));
          }
        }
        // local file system import path elements: current dir, JYTHON_HOME/Lib
        Py.getSystemState().path.append(new PyString(System.getProperty("user.dir")));
        String jyhome = System.getenv("JYTHON_HOME");
        if (jyhome != null) {
          Py.getSystemState().path.append(new PyString(jyhome + File.separator + "Lib"));
        }
        LOG.info(ClassLoader.getSystemResource("python"));
        Py.getSystemState().path.append(new PyString(ClassLoader.getSystemResource("python").toString()));
      } catch (Exception e) {
        LOG.warn("issue with jython cache dir", e);
      }

      // cacdedir now configured, allocate the python interpreter
      interpreter = new PythonInterpreter();
    }

    /**
     * ensure the decorator functions are defined in the interpreter, and
     * manage the module import dependencies.
     * @param path       location of a file to exec in the interpreter
     * @param queryContext if non-null, module import state is tracked
     * @throws IOException
     */
    static synchronized void init(OptionalFunctionContext context, String path) throws IOException {
      // Decorators -
      // "schemaFunction"
      // "outputSchema"
      // "outputSchemaFunction"

      if (!filesLoaded.contains(path)) {
        // attempt addition of schema decorator handler, fail silently
        interpreter.exec("def outputSchema(schema_def):\n"
            + "    def decorator(func):\n"
            + "        func.outputSchema = schema_def\n"
            + "        return func\n"
            + "    return decorator\n\n");

        interpreter.exec("def outputSchemaFunction(schema_def):\n"
            + "    def decorator(func):\n"
            + "        func.outputSchemaFunction = schema_def\n"
            + "        return func\n"
            + "    return decorator\n");

        interpreter.exec("def schemaFunction(schema_def):\n"
            + "     def decorator(func):\n"
            + "         func.schemaFunction = schema_def\n"
            + "         return func\n"
            + "     return decorator\n\n");

        InputStream is = getScriptAsStream(path);
        if (is == null) {
          throw new IllegalStateException("unable to create a stream for path: " + path);
        }
        try {
          execfile(context, is, path);
        } finally {
          is.close();
        }
      }
    }

    /**
     * does not call script.close()
     * @param script
     * @param path
     * @param queryContext
     * @throws Exception
     */
    static void execfile(OptionalFunctionContext context, InputStream script, String path) throws RuntimeException {
      try {

        if( context != null ) {
          String [] argv;
          try {
//            argv = (String[])ObjectSerializer.deserialize(
//                queryContext.get(ConfVars.PYTHON_CMD_ARGS_REMAINDERS));
            argv = new String [] {};
          } catch (Exception e) {
            throw new RuntimeException("Cannot deserialize command line arguments", e);
          }
          PySystemState  state = Py.getSystemState();
          state.argv.clear();
          if( argv != null ) {
            for (String str : argv ) {
              state.argv.append(new PyString(str));
            }
          } else {
            LOG.warn(ConfVars.PYTHON_CMD_ARGS_REMAINDERS.name()
                + " is empty. This is not expected unless on testing." );
          }
        }

        // determine the current module state
        Map<String, String> before = context != null ? getModuleState() : null;
        if (before != null) {
          // os.py, stax.py and posixpath.py are part of the initial state
          // if Lib directory is present and without including os.py, modules
          // which import os fail
          Set<String> includePyModules = new HashSet<String>();
          for (String key : before.keySet()) {
            // $py.class is created if Lib folder is writable
            if (key.endsWith(".py") || key.endsWith("$py.class")) {
              includePyModules.add(key);
            }
          }
          before.keySet().removeAll(includePyModules);
        }

        LOG.info(ClassLoader.getSystemResource("python").getPath());
        interpreter.getSystemState().path.append(new PyString(ClassLoader.getSystemResource("python").getPath()));

        // exec the code, arbitrary imports are processed
        interpreter.execfile(script, path);

        // determine the 'post import' module state
        Map<String, String> after = context != null ? getModuleState() : null;

        // add the module files to the context
        if (after != null && context != null) {
          after.keySet().removeAll(before.keySet());
          for (Map.Entry<String, String> entry : after.entrySet()) {
            String modulename = entry.getKey();
            String modulepath = entry.getValue();
            if (modulepath.equals(JVM_JAR)) {
              continue;
            } else if (modulepath.endsWith(".jar") || modulepath.endsWith(".zip")) {
//              context.addScriptJar(modulepath);
              throw new RuntimeException("jar and zip script files are not supported");
            } else {
              context.addScriptFile(modulename, modulepath);
            }
          }
        }
      } catch (PyException e) {
        if (e.match(Py.SystemExit)) {
          PyObject value = e.value;
          if (PyException.isExceptionInstance(e.value)) {
            value = value.__findattr__("code");
          }
          if (new  PyInteger(0).equals(value)) {
            LOG.info("Script invoked sys.exit(0)");
            return;
          }
        }
        String message = "Python Error. " + e;
        //throw new ExecException(message, 1121, e);
        throw new RuntimeException(message, e);
      }
    }

    static String get(String name) {
      return interpreter.get(name).toString();
    }

    static void setMain(boolean isMain) {
      if (isMain) {
        interpreter.set("__name__", "__main__");
      } else {
        interpreter.set("__name__", "__lib__");
      }
    }

    /**
     * get the state of modules currently loaded
     * @return a map of module name to module file (absolute path)
     */
    private static Map<String, String> getModuleState() {
      // determine the current module state
      Map<String, String> files = new HashMap<String, String>();
      PyStringMap modules = (PyStringMap) Py.getSystemState().modules;
      for (PyObject kvp : modules.iteritems().asIterable()) {
        PyTuple tuple = (PyTuple) kvp;
        String name = tuple.get(0).toString();
        Object value = tuple.get(1);
        // inspect the module to determine file location and status
        try {
          Object fileEntry = null;
          Object loader = null;
          if (value instanceof PyJavaPackage ) {
            fileEntry = ((PyJavaPackage) value).__file__;
          } else if (value instanceof PyObject) {
            // resolved through the filesystem (or built-in)
            PyObject dict = ((PyObject) value).getDict();
            if (dict != null) {
              fileEntry = dict.__finditem__("__file__");
              loader = dict.__finditem__("__loader__");
            } // else built-in
          }   // else some system module?

          if (fileEntry != null) {
            File file = resolvePyModulePath(fileEntry.toString(), loader);
            if (file.exists()) {
              String apath = file.getAbsolutePath();
              if (apath.endsWith(".jar") || apath.endsWith(".zip")) {
                // jar files are simple added to the pigContext
                files.put(apath, apath);
              } else {
                // determine the relative path that the file should have in the jar
                int pos = apath.lastIndexOf(File.separatorChar + name.replace('.', File.separatorChar));
                if (pos > 0) {
                  files.put(apath.substring(pos + 1), apath);
                } else {
                  files.put(apath, apath);
                }
              }
            } else {
              LOG.warn("module file does not exist: " + name + ", " + file);
            }
          } // else built-in
        } catch (Exception e) {
          LOG.warn("exception while retrieving module state: " + value, e);
        }
      }
      return files;
    }
  }

  private static File resolvePyModulePath(String path, Object loader) {
    File file = new File(path);
    if (!file.exists() && loader != null) {
      if(path.startsWith(ClasspathPyImporter.PYCLASSPATH_PREFIX) && loader instanceof ClasspathPyImporter) {
        path = path.replaceFirst(ClasspathPyImporter.PYCLASSPATH_PREFIX, "");
        URL resource = ScriptEngine.class.getResource(path);
        if (resource == null) {
          resource = ScriptEngine.class.getResource(File.separator + path);
        }
        if (resource != null) {
          return new File(resource.getFile());
        }
      } else if (loader instanceof zipimporter) {
        zipimporter importer = (zipimporter) loader;
        return new File(importer.archive);
      } //JavaImporter??
    }
    return file;
  }

  /**
   * Gets the Python function object.
   * @param path Path of the jython script file containing the function.
   * @param functionName Name of the function
   * @return a function object
   * @throws IOException
   */
  public static PyFunction getFunction(String path, String functionName) throws IOException {
    Interpreter.setMain(false);
    Interpreter.init(null, path);
    return (PyFunction) Interpreter.interpreter.get(functionName);
  }

  @Override
  protected String getScriptingLang() {
    return "jython";
  }

  @Override
  protected Map<String, Object> getParamsFromVariables() throws IOException {
    PyFrame frame = Py.getFrame();
    @SuppressWarnings("unchecked")
    List<PyTuple> locals = ((PyStringMap) frame.getLocals()).items();
    Map<String, Object> vars = new HashMap<String, Object>();
    for (PyTuple item : locals) {
      String key = (String) item.get(0);
      Object obj = item.get(1);
      if (obj != null) {
        String value = item.get(1).toString();
        vars.put(key, value);
      }
    }
    return vars;
  }

  private static final Pattern p = Pattern.compile("^\\s*def\\s+(\\w+)\\s*.+");
  private static final Pattern p1 = Pattern.compile("^\\s*if\\s+__name__\\s+==\\s+[\"']__main__[\"']\\s*:\\s*$");

  private static boolean hasFunction(InputStream is) throws IOException {
    boolean hasFunction = false;
    boolean hasMain = false;
    InputStreamReader in = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(in);
    String line = br.readLine();
    while (line != null) {
      if (p.matcher(line).matches()) {
        hasFunction = true;
      } else if (p1.matcher(line).matches()) {
        hasMain = true;
      }
      line = br.readLine();
    }
    if (hasFunction && !hasMain) {
      String msg = "Embedded script cannot mix UDFs with top level code. "
          + "Please use if __name__ == '__main__': construct";
      throw new IOException(msg);
    }
    return hasFunction;
  }

  /**
   * File.deleteOnExit(File) does not work for a non-empty directory. This
   * Thread is used to clean up the python.cachedir (if it was a tmp dir
   * created by the Engine)
   */
  private static class DirDeleter extends Thread {
    private final File dir;
    public DirDeleter(final File file) {
      dir = file;
    }
    @Override
    public void run() {
      try {
        delete(dir);
      } catch (Exception e) {
        LOG.warn("on cleanup", e);
      }
    }
    private static boolean delete(final File file) {
      if (file.isDirectory()) {
        for (File f : file.listFiles()) {
          delete(f);
        }
      }
      return file.delete();
    }
  }

  //  @Override
//  public void registerFunctions(String path, String namespace, QueryContext context)
  public static Set<FunctionDesc> registerFunctions(OptionalFunctionContext context, String path, String namespace)
      throws IOException {
    Interpreter.setMain(false);
    Interpreter.init(context, path);
//    context.addScriptJar(getJarPath(PythonInterpreter.class));
    PythonInterpreter pi = Interpreter.interpreter;
    @SuppressWarnings("unchecked")
    List<PyTuple> locals = ((PyStringMap) pi.getLocals()).items();
    namespace = (namespace == null) ? "" : namespace + NAMESPACE_SEPARATOR;
    Set<FunctionDesc> functionDescs = TUtil.newHashSet();

    for (PyTuple item : locals) {
      String key = (String) item.get(0);
      Object value = item.get(1);
      if (!key.startsWith("__") && !key.equals("schemaFunction")
          && !key.equals("outputSchema")
          && !key.equals("outputSchemaFunction")
          && (value instanceof PyFunction)
          && (((PyFunction)value).__findattr__("schemaFunction".intern())== null)) {
        PyFunction pyFunction = (PyFunction) value;
        PyObject obj = pyFunction.__findattr__("outputSchema".intern());
        TajoDataTypes.Type returnType;
        if(obj != null) {
//            Utils.getSchemaFromString(obj.toString());
          LOG.info("outputSchema: " + obj.toString());
          String[] types = obj.toString().split(",");
          if (types.length > 1) {
            throw new IOException("Multiple return type is not supported");
          }
          returnType = TajoDataTypes.Type.valueOf(types[0].trim().toUpperCase());
        } else {
          // the default return type is the byte array
          returnType = TajoDataTypes.Type.BLOB;
        }
        int paramNum = ((PyBaseCode) pyFunction.__code__).co_argcount;
        LOG.info("co_argcount: " + paramNum);
        TajoDataTypes.DataType[] paramTypes = new TajoDataTypes.DataType[paramNum];
        for (int i = 0; i < paramNum; i++) {
          paramTypes[i] = TajoDataTypes.DataType.newBuilder().setType(TajoDataTypes.Type.ANY).build();
        }

        FunctionSignature signature = new FunctionSignature(CatalogProtos.FunctionType.UDF, key,
            TajoDataTypes.DataType.newBuilder().setType(returnType).build(), paramTypes);
        FunctionInvocation invocation = new FunctionInvocation();
        invocation.setPython(new PythonInvocationDesc(key, path));
        FunctionSupplement supplement = new FunctionSupplement();
        functionDescs.add(new FunctionDesc(signature, invocation, supplement));
        LOG.info("Register scripting UDF: " + namespace + key);
      }
    }

    context.addScriptFile(path);
    Interpreter.setMain(true);
    return functionDescs;
  }
}

