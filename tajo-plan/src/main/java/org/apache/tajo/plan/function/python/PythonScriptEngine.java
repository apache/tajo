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
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.function.FunctionInvocation;
import org.apache.tajo.function.FunctionSignature;
import org.apache.tajo.function.FunctionSupplement;
import org.apache.tajo.function.PythonInvocationDesc;
import org.apache.tajo.plan.function.FunctionContext;
import org.apache.tajo.plan.function.PythonAggFunctionInvoke.PythonAggFunctionContext;
import org.apache.tajo.plan.function.stream.*;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.FileUtil;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
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

    Set<FunctionDesc> functionDescs = new HashSet<>();

    List<FunctionInfo> functions = null;
    try (InputStream in = getScriptAsStream(path)) {
      functions = getFunctions(in);
    }

    for(FunctionInfo funcInfo : functions) {
      try {
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
      } catch (Throwable t) {
        // ignore invalid functions
        LOG.warn(t);
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
    List<FunctionInfo> functions = new ArrayList<>();
    InputStreamReader in = new InputStreamReader(is, Charset.defaultCharset());
    BufferedReader br = new BufferedReader(in);
    String line = br.readLine();
    String[] quotedSchemaStrings = null;
    AggFuncInfo aggFuncInfo = null;

    while (line != null) {
      try {
        if (pSchema.matcher(line).matches()) {
          int start = line.indexOf("(") + 1; //drop brackets
          int end = line.lastIndexOf(")");
          quotedSchemaStrings = line.substring(start, end).trim().split(",");
        } else if (pDef.matcher(line).matches()) {
          boolean isUdaf = aggFuncInfo != null && line.indexOf("def ") > 0;
          int nameStart = line.indexOf("def ") + "def ".length();
          int nameEnd = line.indexOf('(');
          int signatureEnd = line.indexOf(')');
          String[] params = line.substring(nameEnd + 1, signatureEnd).split(",");
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
          quotedSchemaStrings = quotedSchemaStrings == null ? new String[]{"'blob'"} : quotedSchemaStrings;

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
      } catch (Throwable t) {
        // ignore unexpected function and source lines
        LOG.warn(t);
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
  private static final String TAJO_UTIL_NAME = "tajo_util.py";
  private static final String CONTROLLER_NAME = "controller.py";
  private static final String BASE_DIR = FileUtils.getTempDirectoryPath() + File.separator + "tajo-" + System.getProperty("user.name") + File.separator + "python";
  private static final String PYTHON_CONTROLLER_JAR_PATH = "/python/" + CONTROLLER_NAME; // Relative to root of tajo jar.
  private static final String PYTHON_TAJO_UTIL_JAR_PATH = "/python/" + TAJO_UTIL_NAME; // Relative to root of tajo jar.

  // Indexes for arguments being passed to external process
  enum COMMAND_IDX {
    UDF_LANGUAGE,
    PATH_TO_CONTROLLER_FILE,
    UDF_FILE_NAME,
    UDF_FILE_PATH,
    PATH_TO_FILE_CACHE,
    OUT_SCHEMA,
    FUNCTION_OR_CLASS_NAME,
    FUNCTION_TYPE,
  }

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
    FileUtil.cleanup(LOG, stdin, stdout, stderr, inputHandler, outputHandler);
    stdin = null;
    stdout = stderr = null;
    inputHandler = null;
    outputHandler = null;

    try {
      int exitCode = process.waitFor();

      if (systemConf.get(TajoConstants.TEST_KEY, Boolean.FALSE.toString()).equalsIgnoreCase(Boolean.TRUE.toString())) {
        LOG.warn("Process exit code: " + exitCode);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Process exit code: " + exitCode);
        }
      }
    } catch (InterruptedException e) {
      LOG.warn(e.getMessage(), e);
    }

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
    String[] command = new String[8];

    String funcName = invocationDesc.getName();
    String filePath = invocationDesc.getPath();

    command[COMMAND_IDX.UDF_LANGUAGE.ordinal()] = PYTHON_LANGUAGE;
    command[COMMAND_IDX.PATH_TO_CONTROLLER_FILE.ordinal()] = getControllerPath();
    int lastSeparator = filePath.lastIndexOf(File.separator) + 1;
    String fileName = filePath.substring(lastSeparator);
    fileName = fileName.endsWith(FILE_EXTENSION) ? fileName.substring(0, fileName.length()-3) : fileName;
    command[COMMAND_IDX.UDF_FILE_NAME.ordinal()] = fileName;
    command[COMMAND_IDX.UDF_FILE_PATH.ordinal()] = lastSeparator <= 0 ? "." : filePath.substring(0, lastSeparator - 1);
    String fileCachePath = systemConf.get(TajoConf.ConfVars.PYTHON_CODE_DIR.keyname());
    if (fileCachePath == null) {
      throw new IOException(TajoConf.ConfVars.PYTHON_CODE_DIR.keyname() + " must be set.");
    }
    command[COMMAND_IDX.PATH_TO_FILE_CACHE.ordinal()] = "'" + fileCachePath + "'";
    command[COMMAND_IDX.OUT_SCHEMA.ordinal()] = outSchema.getColumn(0).getDataType().getType().name().toLowerCase();
    command[COMMAND_IDX.FUNCTION_OR_CLASS_NAME.ordinal()] = funcName;
    command[COMMAND_IDX.FUNCTION_TYPE.ordinal()] = invocationDesc.isScalarFunction() ? "UDF" : "UDAF";

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

  private static final File pythonScriptBaseDir = new File(PythonScriptEngine.getBaseDirPath());
  private static final File pythonScriptControllerCopy = new File(PythonScriptEngine.getControllerPath());
  private static final File pythonScriptUtilCopy = new File(PythonScriptEngine.getTajoUtilPath());

  public static void initPythonScriptEngineFiles() throws IOException {
    if (!pythonScriptBaseDir.exists()) {
      pythonScriptBaseDir.mkdirs();
    }
    // Controller and util should be always overwritten.
    PythonScriptEngine.loadController(pythonScriptControllerCopy);
    PythonScriptEngine.loadTajoUtil(pythonScriptUtilCopy);
  }

  public static void loadController(File controllerCopy) throws IOException {
    try (InputStream controllerInputStream = PythonScriptEngine.class.getResourceAsStream(PYTHON_CONTROLLER_JAR_PATH)) {
      FileUtils.copyInputStreamToFile(controllerInputStream, controllerCopy);
    }
  }

  public static void loadTajoUtil(File utilCopy) throws IOException {
    try (InputStream utilInputStream = PythonScriptEngine.class.getResourceAsStream(PYTHON_TAJO_UTIL_JAR_PATH)) {
      FileUtils.copyInputStreamToFile(utilInputStream, utilCopy);
    }
  }

  public static String getBaseDirPath() {
    LOG.info("Python base dir is " + BASE_DIR);
    return BASE_DIR;
  }

  /**
   * Find the path to the controller file for the streaming language.
   *
   * @return
   * @throws IOException
   */
  public static String getControllerPath() {
    return BASE_DIR + File.separator + CONTROLLER_NAME;
  }

  public static String getTajoUtilPath() {
    return BASE_DIR + File.separator + TAJO_UTIL_NAME;
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
    } catch (Throwable e) {
      throwException(stderr, new RuntimeException("Failed adding input to inputQueue", e));
    }

    Datum result = null;
    try {
      Tuple next = outputHandler.getNext();
      if (next != null) {
        result = next.asDatum(0);
      } else {
        throw new RuntimeException("Cannot get output result from python controller");
      }
    } catch (Throwable e) {
      throwException(stderr, new RuntimeException("Problem getting output: " + e.getMessage(), e));
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
    } catch (Throwable e) {
      throwException(stderr, new RuntimeException("Failed adding input to inputQueue while executing "
          + methodName + " with " + input, e));
    }

    try {
      outputHandler.getNext();
    } catch (Exception e) {
      throwException(stderr, new RuntimeException("Problem getting output: " + e.getMessage(), e));
    }
  }

  /**
   * Get the standard error streams of the external process and throw the exception
   *
   * @throws RuntimeException
   */
  private void throwException(InputStream stderr, RuntimeException e) throws RuntimeException {
    try {
      if (stderr.available() > 0) {
        byte[] bytes = new byte[Math.min(stderr.available(), 100 * StorageUnit.KB)];
        IOUtils.readFully(stderr, bytes);
        String message = new String(bytes, Charset.defaultCharset());

        throw new RuntimeException("Python exception caused by: " + message, e);
      } else {
        throw e;
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe.getMessage(), ioe);
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
    } catch (Throwable e) {
      throwException(stderr, new RuntimeException("Failed adding input to inputQueue", e));
    }
    try {
      outputHandler.getNext();
    } catch (Throwable e) {
      throwException(stderr, new RuntimeException("Problem getting output: " + e.getMessage(), e));
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
    } catch (Throwable e) {
      throwException(stderr, new RuntimeException("Failed adding input to inputQueue", e));
    }
    try {
      outputHandler.getNext(functionContext);
    } catch (Throwable e) {
      throwException(stderr, new RuntimeException("Problem getting output: " + e.getMessage(), e));
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
    } catch (Throwable e) {
      throwException(stderr, new RuntimeException("Failed adding input to inputQueue", e));
    }
    String result = null;
    try {
      result = outputHandler.getPartialResultString();
    } catch (Throwable e) {
      throwException(stderr, new RuntimeException("Problem getting output: " + e.getMessage(), e));
    }
    return result;
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
      throwException(stderr, new RuntimeException("Failed adding input to inputQueue", e));
    }
    Datum result = null;
    try {
      Tuple next = outputHandler.getNext();
      if (next != null) {
        result = next.asDatum(0);
      } else {
        throw new RuntimeException("Cannot get output result from python controller");
      }
    } catch (Exception e) {
      throwException(stderr, new RuntimeException("Problem getting output: " + e.getMessage(), e));
    }

    return result;
  }
}
