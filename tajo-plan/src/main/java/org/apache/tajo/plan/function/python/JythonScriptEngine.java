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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.function.FunctionInvocation;
import org.apache.tajo.function.FunctionSignature;
import org.apache.tajo.function.FunctionSupplement;
import org.apache.tajo.function.PythonInvocationDesc;
import org.apache.tajo.util.TUtil;
import org.python.core.*;
import org.python.util.PythonInterpreter;

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
            if (!tmp.canWrite()) {
              LOG.error("CACHEDIR: not writable");
              throw new RuntimeException("python.cachedir not writable: " + cdir);
            }
          }
          if (tmp == null) {
            tmp = File.createTempFile("tajo_jython_", "");
            tmp.delete();
            if (!tmp.mkdirs()) {
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
      } catch (Exception e) {
        LOG.warn("issue with jython cache dir", e);
      }

      // cacdedir now configured, allocate the python interpreter
      interpreter = new PythonInterpreter();
    }

    /**
     * Ensure the decorator functions are defined in the interpreter, and
     * manage the module import dependencies.
     * @param initPhase   True if the script is not registered. Otherwise false.
     * @param path        location of a script file to exec in the interpreter
     * @throws IOException
     */
    static synchronized void init(boolean initPhase, String path) throws IOException {
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

        // TODO: Currently, we don't support the customized output schema feature.
//        interpreter.exec("def outputSchemaFunction(schema_def):\n"
//            + "    def decorator(func):\n"
//            + "        func.outputSchemaFunction = schema_def\n"
//            + "        return func\n"
//            + "    return decorator\n");
//
//        interpreter.exec("def schemaFunction(schema_def):\n"
//            + "     def decorator(func):\n"
//            + "         func.schemaFunction = schema_def\n"
//            + "         return func\n"
//            + "     return decorator\n\n");

        InputStream is = getScriptAsStream(path);
        if (is == null) {
          throw new IllegalStateException("unable to create a stream for path: " + path);
        }
        try {
          execfile(initPhase, is, path);
        } finally {
          is.close();
        }
      }
    }

    /**
     * does not call script.close()
     * @param initPhase   True if the script is not registered. Otherwise false.
     * @param script      Input stream to the script file
     * @param path        Path to the script file
     * @throws Exception
     */
    static void execfile(boolean initPhase, InputStream script, String path) throws RuntimeException {
      try {
        // exec the code, arbitrary imports are processed
        interpreter.execfile(script, path);
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
        throw new RuntimeException(message, e);
      }
    }

    static void setMain(boolean isMain) {
      if (isMain) {
        interpreter.set("__name__", "__main__");
      } else {
        interpreter.set("__name__", "__lib__");
      }
    }
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
    Interpreter.init(false, path);
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

  public static Set<FunctionDesc> registerFunctions(String path, String namespace)
      throws IOException {
    Interpreter.setMain(false);
    Interpreter.init(true, path);
    PythonInterpreter pi = Interpreter.interpreter;
    @SuppressWarnings("unchecked")
    List<PyTuple> locals = ((PyStringMap) pi.getLocals()).items();
    namespace = (namespace == null) ? "" : namespace + NAMESPACE_SEPARATOR;
    Set<FunctionDesc> functionDescs = TUtil.newHashSet();

    for (PyTuple item : locals) {
      String key = (String) item.get(0);
      Object value = item.get(1);
      if (!key.startsWith(JythonConstants.SKIP_TOKEN) && !key.equals(JythonConstants.SCHEMA_FUNCTION)
          && !key.equals(JythonConstants.OUTPUT_SCHEMA)
          && !key.equals(JythonConstants.OUTPUT_SCHEMA_FUNCTION)
          && (value instanceof PyFunction)
          && (((PyFunction)value).__findattr__(JythonConstants.SCHEMA_FUNCTION)== null)) {
        PyFunction pyFunction = (PyFunction) value;

        // Find the pre-defined output schema
        TajoDataTypes.Type returnType;
        PyObject obj = pyFunction.__findattr__(JythonConstants.SCHEMA_FUNCTION);
        if (obj != null) {
          returnType = pyObjectToType(obj);
        } else {
          // the default return type is the byte array
          returnType = TajoDataTypes.Type.BLOB;
        }

        int paramNum = ((PyBaseCode) pyFunction.__code__).co_argcount;
        TajoDataTypes.DataType[] paramTypes = new TajoDataTypes.DataType[paramNum];
        for (int i = 0; i < paramNum; i++) {
          paramTypes[i] = TajoDataTypes.DataType.newBuilder().setType(TajoDataTypes.Type.ANY).build();
        }

        FunctionSignature signature = new FunctionSignature(CatalogProtos.FunctionType.UDF, key,
            TajoDataTypes.DataType.newBuilder().setType(returnType).build(), paramTypes);
        FunctionInvocation invocation = new FunctionInvocation();
        PythonInvocationDesc invocationDesc = new PythonInvocationDesc(key, path);
        invocation.setPython(invocationDesc);
        FunctionSupplement supplement = new FunctionSupplement();
        functionDescs.add(new FunctionDesc(signature, invocation, supplement));
        LOG.info("Register scripting UDF: " + namespace + key);
      }
    }

    Interpreter.setMain(true);
    return functionDescs;
  }

  private static TajoDataTypes.Type pyObjectToType(PyObject obj) {
    return TajoDataTypes.Type.valueOf(pyObjectToTypeStringCand(obj).toUpperCase());
  }

  private static String pyObjectToTypeStringCand(PyObject obj) {
    String[] types = obj.toString().split(",");
    if (types.length > 1) {
      throw new UnsupportedException("Multiple return type is not supported");
    }
    return types[0].trim();
  }
}

