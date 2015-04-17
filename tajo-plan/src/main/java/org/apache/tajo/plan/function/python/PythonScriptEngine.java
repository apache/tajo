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
import org.apache.tajo.function.FunctionInvocation;
import org.apache.tajo.function.FunctionSignature;
import org.apache.tajo.function.FunctionSupplement;
import org.apache.tajo.function.PythonInvocationDesc;
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
    List<FuncInfo> functions = null;
    try {
      functions = getFunctions(in);
    } finally {
      in.close();
    }
    for(FuncInfo funcInfo : functions) {
      TajoDataTypes.DataType returnType = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.valueOf(funcInfo.returnType));
      FunctionSignature signature = new FunctionSignature(CatalogProtos.FunctionType.UDF, funcInfo.funcName,
          returnType, createParamTypes(funcInfo.paramNum));
      FunctionInvocation invocation = new FunctionInvocation();
      PythonInvocationDesc invocationDesc = new PythonInvocationDesc(funcInfo.funcName, path.getPath());
      invocation.setPython(invocationDesc);
      FunctionSupplement supplement = new FunctionSupplement();
      functionDescs.add(new FunctionDesc(signature, invocation, supplement));
    }
    return functionDescs;
  }

  private static TajoDataTypes.DataType[] createParamTypes(int paramNum) {
    TajoDataTypes.DataType[] paramTypes = new TajoDataTypes.DataType[paramNum];
    for (int i = 0; i < paramNum; i++) {
      paramTypes[i] = TajoDataTypes.DataType.newBuilder().setType(TajoDataTypes.Type.ANY).build();
    }
    return paramTypes;
  }

  private static final Pattern pSchema = Pattern.compile("^\\s*\\W+outputType.*");
  private static final Pattern pDef = Pattern.compile("^\\s*def\\s+(\\w+)\\s*.+");

  private static class FuncInfo {
    String returnType;
    String funcName;
    int paramNum;

    public FuncInfo(String returnType, String funcName, int paramNum) {
      this.returnType = returnType.toUpperCase();
      this.funcName = funcName;
      this.paramNum = paramNum;
    }
  }

  // TODO: python parser must be improved.
  private static List<FuncInfo> getFunctions(InputStream is) throws IOException {
    List<FuncInfo> functions = TUtil.newList();
    InputStreamReader in = new InputStreamReader(is, Charset.defaultCharset());
    BufferedReader br = new BufferedReader(in);
    String line = br.readLine();
    String schemaString = null;
    while (line != null) {
      if (pSchema.matcher(line).matches()) {
        int start = line.indexOf("(") + 2; //drop brackets/quotes
        int end = line.lastIndexOf(")") - 1;
        schemaString = line.substring(start,end).trim();
      } else if (pDef.matcher(line).matches()) {
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

        String functionName = line.substring(nameStart, nameEnd).trim();
        schemaString = schemaString == null ? "blob" : schemaString;
        functions.add(new FuncInfo(schemaString, functionName, paramNum));
        schemaString = null;
      }
      line = br.readLine();
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
  private static final int UDF_NAME = 4; // Name of UDF function being called.
  private static final int PATH_TO_FILE_CACHE = 5; // Directory where required files (like tajo_util) are cached on cluster nodes.
  private static final int STD_OUT_OUTPUT_PATH = 6; // File for output from when user writes to standard output.
  private static final int STD_ERR_OUTPUT_PATH = 7; // File for output from when user writes to standard error.
  private static final int CONTROLLER_LOG_FILE_PATH = 8; // Controller log file logs progress through the controller script not user code.
  private static final int OUT_SCHEMA = 9; // the schema of the output column

  private Configuration systemConf;

  private Process process; // Handle to the external execution of python functions

  private InputHandler inputHandler;
  private OutputHandler outputHandler;

  private DataOutputStream stdin; // stdin of the process
  private InputStream stdout; // stdout of the process
  private InputStream stderr; // stderr of the process

  private final FunctionSignature functionSignature;
  private final PythonInvocationDesc invocationDesc;
  private final Schema inSchema;
  private final Schema outSchema;
  private final int [] projectionCols = new int[]{0};

  private final CSVLineSerDe lineSerDe = new CSVLineSerDe();
  private final TableMeta pipeMeta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.TEXTFILE);

  private static final Tuple EMPTY_INPUT = new VTuple(0);

  public PythonScriptEngine(FunctionDesc functionDesc) {
    if (!functionDesc.getInvocation().hasPython()) {
      throw new IllegalStateException("Function type must be 'python'");
    }
    functionSignature = functionDesc.getSignature();
    invocationDesc = functionDesc.getInvocation().getPython();

    TajoDataTypes.DataType[] paramTypes = functionSignature.getParamTypes();
    inSchema = new Schema();
    for (int i = 0; i < paramTypes.length; i++) {
      inSchema.addColumn(new Column("in_" + i, paramTypes[i]));
    }
    outSchema = new Schema(new Column[]{new Column("out", functionSignature.getReturnType())});
  }

  @Override
  public void start(Configuration systemConf) throws IOException {
    this.systemConf = systemConf;
    startUdfController();
    createInputHandlers();
    setStreams();
    if (LOG.isDebugEnabled()) {
      LOG.debug("PythonScriptExecutor starts up");
    }
  }

  @Override
  public void shutdown() {
    process.destroy();
    FileUtil.cleanup(LOG, stdin);
    FileUtil.cleanup(LOG, stdout);
    FileUtil.cleanup(LOG, stderr);
    FileUtil.cleanup(LOG, inputHandler);
    FileUtil.cleanup(LOG, outputHandler);
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
    String[] command = new String[10];

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
    command[UDF_NAME] = funcName;
    String fileCachePath = systemConf.get(TajoConf.ConfVars.PYTHON_CODE_DIR.keyname());
    if (fileCachePath == null) {
      throw new IOException(TajoConf.ConfVars.PYTHON_CODE_DIR.keyname() + " must be set.");
    }
    command[PATH_TO_FILE_CACHE] = "'" + fileCachePath + "'";
    command[STD_OUT_OUTPUT_PATH] = outFileName;
    command[STD_ERR_OUTPUT_PATH] = errOutFileName;
    command[CONTROLLER_LOG_FILE_PATH] = controllerLogFileName;
    command[OUT_SCHEMA] = outSchema.getColumn(0).getDataType().getType().name().toLowerCase();

    return command;
  }

  private void createInputHandlers() {
    TextLineSerializer serializer = lineSerDe.createSerializer(inSchema, pipeMeta);
    serializer.init();
    this.inputHandler = new InputHandler(serializer);
    TextLineDeserializer deserializer = lineSerDe.createDeserializer(outSchema, pipeMeta, projectionCols);
    deserializer.init();
    this.outputHandler = new OutputHandler(deserializer);
  }

  /**
   * Get the standard input, output, and error streams of the external process
   *
   * @throws IOException
   */
  private void setStreams() throws IOException {
    stdout = new DataInputStream(new BufferedInputStream(process.getInputStream()));
    outputHandler.bindTo(stdout);

    stdin = new DataOutputStream(new BufferedOutputStream(process.getOutputStream()));
    inputHandler.bindTo(stdin);

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

  public Datum eval(Tuple input) {
    try {
      if (input == null) {
        // When nothing is passed into the UDF the tuple
        // being sent is the full tuple for the relation.
        // We want it to be nothing (since that's what the user wrote).
        input = EMPTY_INPUT;
      }

      inputHandler.putNext(input);
      stdin.flush();
    } catch (Exception e) {
      throw new RuntimeException("Failed adding input to inputQueue", e);
    }
    Datum result;
    try {
      result = outputHandler.getNext().get(0);
    } catch (Exception e) {
      throw new RuntimeException("Problem getting output", e);
    }

    return result;
  }
}
