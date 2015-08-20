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

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.function.*;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.function.PythonAggFunctionInvoke.PythonAggFunctionContext;
import org.apache.tajo.plan.function.stream.*;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.TUtil;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * {@link PythonScriptEngine} is responsible for registering python functions and maintaining the controller process.
 * The controller is a python process that executes the python UDFs.
 * (Please refer to 'tajo-core/src/main/resources/python/controller.py')
 * Data are exchanged via standard I/O between PythonScriptEngine and the controller.
 */
public class PythonScriptEngine extends TajoScriptEngine {

  private static final Log LOG = LogFactory.getLog(PythonScriptEngine.class);

  public static final String FILE_EXTENSION = ".py";

  /**
   * Register functions defined in a python script
   *
   * @param path path to the python script file
   * @param namespace namespace where the functions will be defined
   * @return set of function descriptions
   * @throws IOException
   */
  public static Set<FunctionDesc> registerFunctions(URI path, String namespace) throws IOException {
    // TODO: we should support the namespace for python functions.

    Set<FunctionDesc> functionDescs = TUtil.newHashSet();

    InputStream in = getScriptAsStream(path);
    List<FunctionInfo> functions = null;
    try {
      functions = getFunctions(in);
    } finally {
      in.close();
    }
    for(FunctionInfo funcInfo : functions) {
      FunctionSignature signature;
      FunctionInvocation invocation = new FunctionInvocation();
      FunctionSupplement supplement = new FunctionSupplement();
      if (funcInfo instanceof ScalarFuncInfo) {
        ScalarFuncInfo scalarFuncInfo = (ScalarFuncInfo) funcInfo;
        TajoDataTypes.DataType returnType = getReturnTypes(scalarFuncInfo)[0];
        signature = new FunctionSignature(CatalogProtos.FunctionType.UDF, scalarFuncInfo.funcName,
            returnType, createParamTypes(scalarFuncInfo.paramNum));
        PythonInvocationDesc invocationDesc = new PythonInvocationDesc(scalarFuncInfo.funcName, path.getPath(), true);
        invocation.setPython(invocationDesc);
        functionDescs.add(new FunctionDesc(signature, invocation, supplement));
      } else {
        AggFuncInfo aggFuncInfo = (AggFuncInfo) funcInfo;
        if (isValidUdaf(aggFuncInfo)) {
          TajoDataTypes.DataType returnType = getReturnTypes(aggFuncInfo.getFinalResultInfo)[0];
          signature = new FunctionSignature(CatalogProtos.FunctionType.UDA, aggFuncInfo.funcName,
              returnType, createParamTypes(aggFuncInfo.evalInfo.paramNum));
          PythonInvocationDesc invocationDesc = new PythonInvocationDesc(aggFuncInfo.className, path.getPath(), false);
          invocation.setPythonAggregation(invocationDesc);
          functionDescs.add(new FunctionDesc(signature, invocation, supplement));
        }
      }
    }
    return functionDescs;
  }

  private static boolean isValidUdaf(AggFuncInfo aggFuncInfo) {
    if (aggFuncInfo.className != null && aggFuncInfo.evalInfo != null && aggFuncInfo.mergeInfo != null
      && aggFuncInfo.getPartialResultInfo != null && aggFuncInfo.getFinalResultInfo != null) {
      return true;
    }
    return false;
  }

  private static TajoDataTypes.DataType[] createParamTypes(int paramNum) {
    TajoDataTypes.DataType[] paramTypes = new TajoDataTypes.DataType[paramNum];
    for (int i = 0; i < paramNum; i++) {
      paramTypes[i] = TajoDataTypes.DataType.newBuilder().setType(TajoDataTypes.Type.ANY).build();
    }
    return paramTypes;
  }

  private static TajoDataTypes.DataType[] getReturnTypes(ScalarFuncInfo scalarFuncInfo) {
    TajoDataTypes.DataType[] returnTypes = new TajoDataTypes.DataType[scalarFuncInfo.returnTypes.length];
    for (int i = 0; i < scalarFuncInfo.returnTypes.length; i++) {
      returnTypes[i] = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.valueOf(scalarFuncInfo.returnTypes[i]));
    }
    return returnTypes;
  }

  private static final Pattern pSchema = Pattern.compile("^\\s*\\W+output_type.*");
  private static final Pattern pDef = Pattern.compile("^\\s*def\\s+(\\w+)\\s*.+");
  private static final Pattern pClass = Pattern.compile("class.*");

  private interface FunctionInfo {

  }

  private static class AggFuncInfo implements FunctionInfo {
    String className;
    String funcName;
    ScalarFuncInfo evalInfo;
    ScalarFuncInfo mergeInfo;
    ScalarFuncInfo getPartialResultInfo;
    ScalarFuncInfo getFinalResultInfo;
  }

  private static class ScalarFuncInfo implements FunctionInfo {
    String[] returnTypes;
    String funcName;
    int paramNum;

    public ScalarFuncInfo(String[] quotedSchemaStrings, String funcName, int paramNum) {
      this.returnTypes = new String[quotedSchemaStrings.length];
      for (int i = 0; i < quotedSchemaStrings.length; i++) {
        String quotedString = quotedSchemaStrings[i].trim();
        String[] tokens = quotedString.substring(1, quotedString.length()-1).split(":");
        this.returnTypes[i] = tokens.length == 1 ? tokens[0].toUpperCase() : tokens[1].toUpperCase();
      }
      this.funcName = funcName;
      this.paramNum = paramNum;
    }
  }

  // TODO: python parser must be improved.
  private static List<FunctionInfo> getFunctions(InputStream is) throws IOException {
    List<FunctionInfo> functions = TUtil.newList();
    InputStreamReader in = new InputStreamReader(is, Charset.defaultCharset());
    BufferedReader br = new BufferedReader(in);
    String line = br.readLine();
    String[] quotedSchemaStrings = null;
    AggFuncInfo aggFuncInfo = null;
    while (line != null) {
      if (pSchema.matcher(line).matches()) {
        int start = line.indexOf("(") + 1; //drop brackets
        int end = line.lastIndexOf(")");
        quotedSchemaStrings = line.substring(start,end).trim().split(",");
      } else if (pDef.matcher(line).matches()) {
        boolean isUdaf = aggFuncInfo != null && line.indexOf("def ") > 0;
        int nameStart = line.indexOf("def ") + "def ".length();
        int nameEnd = line.indexOf('(');
        int signatureEnd = line.indexOf(')');
        String[] params = line.substring(nameEnd+1, signatureEnd).split(",");
        int paramNum;
        if (params.length == 1) {
          paramNum = params[0].equals("") ? 0 : 1;
        } else {
          paramNum = params.length;
        }
        if (params[0].trim().equals("self")) {
          paramNum--;
        }

        String functionName = line.substring(nameStart, nameEnd).trim();
        quotedSchemaStrings = quotedSchemaStrings == null ? new String[] {"'blob'"} : quotedSchemaStrings;

        if (isUdaf) {
          if (functionName.equals("eval")) {
            aggFuncInfo.evalInfo = new ScalarFuncInfo(quotedSchemaStrings, functionName, paramNum);
          } else if (functionName.equals("merge")) {
            aggFuncInfo.mergeInfo = new ScalarFuncInfo(quotedSchemaStrings, functionName, paramNum);
          } else if (functionName.equals("get_partial_result")) {
            aggFuncInfo.getPartialResultInfo = new ScalarFuncInfo(quotedSchemaStrings, functionName, paramNum);
          } else if (functionName.equals("get_final_result")) {
            aggFuncInfo.getFinalResultInfo = new ScalarFuncInfo(quotedSchemaStrings, functionName, paramNum);
          }
        } else {
          aggFuncInfo = null;
          functions.add(new ScalarFuncInfo(quotedSchemaStrings, functionName, paramNum));
        }

        quotedSchemaStrings = null;
      } else if (pClass.matcher(line).matches()) {
        // UDAF
        if (aggFuncInfo != null) {
          functions.add(aggFuncInfo);
        }
        aggFuncInfo = new AggFuncInfo();
        int classNameStart = line.indexOf("class ") + "class ".length();
        int classNameEnd = line.indexOf("(");
        if (classNameEnd < 0) {
          classNameEnd = line.indexOf(":");
        }
        aggFuncInfo.className = line.substring(classNameStart, classNameEnd).trim();
        aggFuncInfo.funcName = aggFuncInfo.className.toLowerCase();
      }
      line = br.readLine();
    }
    if (aggFuncInfo != null) {
      functions.add(aggFuncInfo);
    }
    br.close();
    in.close();
    return functions;
  }


  private static final String PYTHON_LANGUAGE = "python";
  private static final String PYTHON_ROOT_PATH = "/python";
  private static final String TAJO_UTIL_NAME = "tajo_util.py";
  private static final String CONTROLLER_NAME = "controller.py";
  private static final String PYTHON_CONTROLLER_JAR_PATH = PYTHON_ROOT_PATH + File.separator + CONTROLLER_NAME; // Relative to root of tajo jar.
  private static final String PYTHON_TAJO_UTIL_PATH = PYTHON_ROOT_PATH + File.separator + TAJO_UTIL_NAME; // Relative to root of tajo jar.
  private static final String DEFAULT_LOG_DIR = "/tmp/tajo-" + System.getProperty("user.name") + "/python";

  // Indexes for arguments being passed to external process
  private static final int UDF_LANGUAGE = 0;
  private static final int PATH_TO_CONTROLLER_FILE = 1;
  private static final int UDF_FILE_NAME = 2; // Name of file where UDF function is defined
  private static final int UDF_FILE_PATH = 3; // Path to directory containing file where UDF function is defined
  private static final int PATH_TO_FILE_CACHE = 4; // Directory where required files (like tajo_util) are cached on cluster nodes.
  private static final int STD_OUT_OUTPUT_PATH = 5; // File for output from when user writes to standard output.
  private static final int STD_ERR_OUTPUT_PATH = 6; // File for output from when user writes to standard error.
  private static final int CONTROLLER_LOG_FILE_PATH = 7; // Controller log file logs progress through the controller script not user code.
  private static final int OUT_SCHEMA = 8; // the schema of the output column
  private static final int FUNCTION_OR_CLASS_NAME = 9; // if FUNCTION_TYPE is UDF, function name; if FUNCTION_TYPE is UDAF, class name.
  private static final int FUNCTION_TYPE = 10; // UDF or UDAF

  private Configuration systemConf;

  private Process process; // Handle to the external execution of python functions

  private InputHandler inputHandler;
  private OutputHandler outputHandler;

  private DataOutputStream stdin; // stdin of the process
  private InputStream stdout; // stdout of the process
  private InputStream stderr; // stderr of the process

  private final FunctionSignature functionSignature;
  private final PythonInvocationDesc invocationDesc;
  private Schema inSchema;
  private Schema outSchema;
  private int[] projectionCols;

  private final CSVLineSerDe lineSerDe = new CSVLineSerDe();
  private final TableMeta pipeMeta = CatalogUtil.newTableMeta("TEXT");

  private final Tuple EMPTY_INPUT = new VTuple(0);
  private final Schema EMPTY_SCHEMA = new Schema();

  public PythonScriptEngine(FunctionDesc functionDesc) {
    if (!functionDesc.getInvocation().hasPython() && !functionDesc.getInvocation().hasPythonAggregation()) {
      throw new IllegalStateException("Function type must be 'python'");
    }
    functionSignature = functionDesc.getSignature();
    invocationDesc = functionDesc.getInvocation().getPython();
    setSchema();
  }

  public PythonScriptEngine(FunctionDesc functionDesc, boolean firstPhase, boolean lastPhase) {
    if (!functionDesc.getInvocation().hasPython() && !functionDesc.getInvocation().hasPythonAggregation()) {
      throw new IllegalStateException("Function type must be 'python'");
    }
    functionSignature = functionDesc.getSignature();
    invocationDesc = functionDesc.getInvocation().getPython();
    this.firstPhase = firstPhase;
    this.lastPhase = lastPhase;
    setSchema();
  }

  @Override
  public void start(Configuration systemConf) throws IOException {
    this.systemConf = systemConf;
    startUdfController();
    setStreams();
    createInputHandlers();
    if (LOG.isDebugEnabled()) {
      LOG.debug("PythonScriptExecutor starts up");
    }
  }

  @Override
  public void shutdown() {
    process.destroy();
    FileUtil.cleanup(LOG, stdin, stdout, stderr, inputHandler, outputHandler);
    stdin = null;
    stdout = stderr = null;
    inputHandler = null;
    outputHandler = null;
    if (LOG.isDebugEnabled()) {
      LOG.debug("PythonScriptExecutor shuts down");
    }
  }

  private void startUdfController() throws IOException {
    ProcessBuilder processBuilder = StreamingUtil.createProcess(buildCommand());
    process = processBuilder.start();
  }

  /**
   * Build a command to execute an external process.
   * @return
   * @throws IOException
   */
  private String[] buildCommand() throws IOException {
    String[] command = new String[11];

    // TODO: support controller logging
    String standardOutputRootWriteLocation = systemConf.get(TajoConf.ConfVars.PYTHON_CONTROLLER_LOG_DIR.keyname(),
        DEFAULT_LOG_DIR);
    if (!standardOutputRootWriteLocation.equals(DEFAULT_LOG_DIR)) {
      LOG.warn("Currently, logging is not supported for the python controller.");
    }
    String controllerLogFileName, outFileName, errOutFileName;

    String funcName = invocationDesc.getName();
    String filePath = invocationDesc.getPath();

    controllerLogFileName = standardOutputRootWriteLocation + funcName + "_controller.log";
    outFileName = standardOutputRootWriteLocation + funcName + ".out";
    errOutFileName = standardOutputRootWriteLocation + funcName + ".err";

    command[UDF_LANGUAGE] = PYTHON_LANGUAGE;
    command[PATH_TO_CONTROLLER_FILE] = getControllerPath();
    int lastSeparator = filePath.lastIndexOf(File.separator) + 1;
    String fileName = filePath.substring(lastSeparator);
    fileName = fileName.endsWith(FILE_EXTENSION) ? fileName.substring(0, fileName.length()-3) : fileName;
    command[UDF_FILE_NAME] = fileName;
    command[UDF_FILE_PATH] = lastSeparator <= 0 ? "." : filePath.substring(0, lastSeparator - 1);
    String fileCachePath = systemConf.get(TajoConf.ConfVars.PYTHON_CODE_DIR.keyname());
    if (fileCachePath == null) {
      throw new IOException(TajoConf.ConfVars.PYTHON_CODE_DIR.keyname() + " must be set.");
    }
    command[PATH_TO_FILE_CACHE] = "'" + fileCachePath + "'";
    command[STD_OUT_OUTPUT_PATH] = outFileName;
    command[STD_ERR_OUTPUT_PATH] = errOutFileName;
    command[CONTROLLER_LOG_FILE_PATH] = controllerLogFileName;
    command[OUT_SCHEMA] = outSchema.getColumn(0).getDataType().getType().name().toLowerCase();
    command[FUNCTION_OR_CLASS_NAME] = funcName;
    command[FUNCTION_TYPE] = invocationDesc.isScalarFunction() ? "UDF" : "UDAF";

    return command;
  }

  private void setSchema() {
    if (invocationDesc.isScalarFunction()) {
      TajoDataTypes.DataType[] paramTypes = functionSignature.getParamTypes();
      inSchema = new Schema();
      for (int i = 0; i < paramTypes.length; i++) {
        inSchema.addColumn(new Column("in_" + i, paramTypes[i]));
      }
      outSchema = new Schema(new Column[]{new Column("out", functionSignature.getReturnType())});
    } else {
      // UDAF
      if (firstPhase) {
        // first phase
        TajoDataTypes.DataType[] paramTypes = functionSignature.getParamTypes();
        inSchema = new Schema();
        for (int i = 0; i < paramTypes.length; i++) {
          inSchema.addColumn(new Column("in_" + i, paramTypes[i]));
        }
        outSchema = new Schema(new Column[]{new Column("json", TajoDataTypes.Type.TEXT)});
      } else if (lastPhase) {
        inSchema = new Schema(new Column[]{new Column("json", TajoDataTypes.Type.TEXT)});
        outSchema = new Schema(new Column[]{new Column("out", functionSignature.getReturnType())});
      } else {
        // intermediate phase
        inSchema = outSchema = new Schema(new Column[]{new Column("json", TajoDataTypes.Type.TEXT)});
      }
    }
    projectionCols = new int[outSchema.size()];
    for (int i = 0; i < outSchema.size(); i++) {
      projectionCols[i] = i;
    }
  }

  private void createInputHandlers() throws IOException {
    setSchema();
    TextLineSerializer serializer = lineSerDe.createSerializer(pipeMeta);
    serializer.init();
    this.inputHandler = new InputHandler(serializer);
    inputHandler.bindTo(stdin);
    TextLineDeserializer deserializer = lineSerDe.createDeserializer(outSchema, pipeMeta, projectionCols);
    deserializer.init();
    this.outputHandler = new OutputHandler(deserializer);
    outputHandler.bindTo(stdout);
  }

  /**
   * Get the standard input, output, and error streams of the external process
   *
   * @throws IOException
   */
  private void setStreams() {
    stdout = new DataInputStream(new BufferedInputStream(process.getInputStream()));
    stdin = new DataOutputStream(new BufferedOutputStream(process.getOutputStream()));
    stderr = new DataInputStream(new BufferedInputStream(process.getErrorStream()));
  }

  /**
   * Find the path to the controller file for the streaming language.
   *
   * First check path to job jar and if the file is not found (like in the
   * case of running hadoop in standalone mode) write the necessary files
   * to temporary files and return that path.
   *
   * @return
   * @throws IOException
   */
  private String getControllerPath() throws IOException {
    String controllerPath = PYTHON_CONTROLLER_JAR_PATH;
    File controller = new File(PYTHON_CONTROLLER_JAR_PATH);
    if (!controller.exists()) {
      File controllerFile = File.createTempFile("controller", FILE_EXTENSION);
      InputStream pythonControllerStream = this.getClass().getResourceAsStream(PYTHON_CONTROLLER_JAR_PATH);
      try {
        FileUtils.copyInputStreamToFile(pythonControllerStream, controllerFile);
      } finally {
        pythonControllerStream.close();
      }
      controllerFile.deleteOnExit();
      File tajoUtilFile = new File(controllerFile.getParent() + File.separator + TAJO_UTIL_NAME);
      tajoUtilFile.deleteOnExit();
      InputStream pythonUtilStream = this.getClass().getResourceAsStream(PYTHON_TAJO_UTIL_PATH);
      try {
        FileUtils.copyInputStreamToFile(pythonUtilStream, tajoUtilFile);
      } finally {
        pythonUtilStream.close();
      }
      controllerPath = controllerFile.getAbsolutePath();
    }
    return controllerPath;
  }

  /**
   * Call Python scalar functions.
   *
   * @param input input tuple
   * @return evaluated result datum
   */
  @Override
  public Datum callScalarFunc(Tuple input) {
    try {
      inputHandler.putNext(input, inSchema);
      stdin.flush();
    } catch (Exception e) {
      throw new RuntimeException("Failed adding input to inputQueue", e);
    }
    Datum result;
    try {
      result = outputHandler.getNext().asDatum(0);
    } catch (Exception e) {
      throw new RuntimeException("Problem getting output: " + e.getMessage(), e);
    }

    return result;
  }

  /**
   * Call Python aggregation functions.
   *
   * @param functionContext python function context
   * @param input input tuple
   */
  @Override
  public void callAggFunc(FunctionContext functionContext, Tuple input) {

    String methodName;
    if (firstPhase) {
      // eval
      methodName = "eval";
    } else {
      // merge
      methodName = "merge";
    }

    try {
      inputHandler.putNext(methodName, input, inSchema);
      stdin.flush();
    } catch (Exception e) {
      throw new RuntimeException("Failed adding input to inputQueue while executing " + methodName + " with " + input, e);
    }

    try {
      outputHandler.getNext();
    } catch (Exception e) {
      throw new RuntimeException("Problem getting output: " + e.getMessage(), e);
    }
  }

  /**
   * Restore the intermediate result in Python UDAF with the snapshot stored in the function context.
   *
   * @param functionContext
   */
  public void updatePythonSideContext(PythonAggFunctionContext functionContext) throws IOException {

    try {
      inputHandler.putNext("update_context", functionContext);
      stdin.flush();
    } catch (Exception e) {
      throw new RuntimeException("Failed adding input to inputQueue", e);
    }
    try {
      outputHandler.getNext();
    } catch (Exception e) {
      throw new RuntimeException("Problem getting output: " + e.getMessage(), e);
    }
  }

  /**
   * Get the snapshot of the intermediate result in the Python UDAF.
   *
   * @param functionContext
   */
  public void updateJavaSideContext(PythonAggFunctionContext functionContext) throws IOException {

    try {
      inputHandler.putNext("get_context", EMPTY_INPUT, EMPTY_SCHEMA);
      stdin.flush();
    } catch (Exception e) {
      throw new RuntimeException("Failed adding input to inputQueue", e);
    }
    try {
      outputHandler.getNext(functionContext);
    } catch (Exception e) {
      throw new RuntimeException("Problem getting output: " + e.getMessage(), e);
    }
  }

  /**
   * Get intermediate result after the first stage.
   *
   * @param functionContext
   * @return
   */
  @Override
  public String getPartialResult(FunctionContext functionContext) {
    try {
      inputHandler.putNext("get_partial_result", EMPTY_INPUT, EMPTY_SCHEMA);
      stdin.flush();
    } catch (Exception e) {
      throw new RuntimeException("Failed adding input to inputQueue", e);
    }
    try {
      return outputHandler.getPartialResultString();
    } catch (Exception e) {
      throw new RuntimeException("Problem getting output: " + e.getMessage(), e);
    }
  }

  /**
   * Get final result after the last stage.
   *
   * @param functionContext
   * @return
   */
  @Override
  public Datum getFinalResult(FunctionContext functionContext) {
    try {
      inputHandler.putNext("get_final_result", EMPTY_INPUT, EMPTY_SCHEMA);
      stdin.flush();
    } catch (Exception e) {
      throw new RuntimeException("Failed adding input to inputQueue", e);
    }
    Datum result;
    try {
      result = outputHandler.getNext().asDatum(0);
    } catch (Exception e) {
      throw new RuntimeException("Problem getting output: " + e.getMessage(), e);
    }

    return result;
  }
}
