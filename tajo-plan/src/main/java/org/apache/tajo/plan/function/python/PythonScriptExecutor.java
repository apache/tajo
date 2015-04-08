/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.function.python;

import com.google.common.base.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.QueryVars;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.function.FunctionSignature;
import org.apache.tajo.function.PythonInvocationDesc;
import org.apache.tajo.plan.function.FunctionInvokeContext;
import org.apache.tajo.plan.function.stream.*;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.io.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * {@link PythonScriptExecutor} is a script executor for python functions.
 * It internally creates a child process which is responsible for executing python codes.
 * Given an input tuple, it sends the tuple to the child process, and then receives a result from that.
 */
public class PythonScriptExecutor {

  private static final Log LOG = LogFactory.getLog(PythonScriptExecutor.class);

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

  private Process process; // Handle to the external execution of python functions
  // all processes
  private ProcessErrorThread stderrThread; // thread to get process stderr
  private ProcessInputThread stdinThread; // thread to send input to process
  private ProcessOutputThread stdoutThread; //thread to read output from process

  private InputHandler inputHandler;
  private OutputHandler outputHandler;

  private BlockingQueue<Tuple> inputQueue;
  private BlockingQueue<Object> outputQueue;

  private DataOutputStream stdin; // stdin of the process
  private InputStream stdout; // stdout of the process
  private InputStream stderr; // stderr of the process

  private static final Object ERROR_OUTPUT = new Object();
  private static final Object NULL_OBJECT = new Object();

  private volatile StreamingUDFException outerrThreadsError;

  private FunctionInvokeContext invokeContext = null;

  private final FunctionSignature functionSignature;
  private final PythonInvocationDesc invocationDesc;
  private final Schema inSchema;
  private final Schema outSchema;
  private final int [] projectionCols;

  private final CSVLineSerDe lineSerDe = new CSVLineSerDe();
  private final TableMeta pipeMeta;

  private boolean isStopped = false;

  public PythonScriptExecutor(FunctionDesc functionDesc) {
    if (!functionDesc.getInvocation().hasPython()) {
      throw new IllegalStateException("Function type must be python");
    }
    functionSignature = functionDesc.getSignature();
    invocationDesc = functionDesc.getInvocation().getPython();

    // Compile input/output schema
    // Note that temporal columns are used.
    TajoDataTypes.DataType[] paramTypes = functionSignature.getParamTypes();
    inSchema = new Schema();
    for (int i = 0; i < paramTypes.length; i++) {
      inSchema.addColumn(new Column("in_" + i, paramTypes[i]));
    }
    outSchema = new Schema(new Column[]{new Column("out", functionSignature.getReturnType())});
    projectionCols = new int[]{0};
    pipeMeta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.TEXTFILE);
  }

  public void start(FunctionInvokeContext context) throws IOException {
    isStopped = false;
    this.invokeContext = context;
    this.inputQueue = new ArrayBlockingQueue<Tuple>(1);
    this.outputQueue = new ArrayBlockingQueue<Object>(2);
    startUdfController();
    createInputHandlers();
    setStreams();
    startThreads();
  }

  public void shutdown() throws IOException, InterruptedException {
    isStopped = true;
    process.destroy();
    if (stdin != null) {
      stdin.close();
    }
    if (stdout != null) {
      stdout.close();
    }
    if (stderr != null) {
      stderr.close();
    }
    inputHandler.close(process);
    outputHandler.close();
    LOG.info("shutdowned");
//    stdinThread.join();
//    stderrThread.join();
//    stdoutThread.join();
  }

  private StreamingCommand startUdfController() throws IOException {
    StreamingCommand sc = new StreamingCommand(buildCommand());
    ProcessBuilder processBuilder = StreamingUtil.createProcess(invokeContext.getQueryContext(), sc);
    process = processBuilder.start();

    Runtime.getRuntime().addShutdownHook(new ProcessKiller());

    return sc;
  }

  /**
   * Build a command to execute external process.
   * @return
   * @throws IOException
   */
  private String[] buildCommand() throws IOException {
    OverridableConf queryContext = invokeContext.getQueryContext();
    String[] command = new String[10];

    // TODO: support controller logging
    String standardOutputRootWriteLocation = "";
    if (queryContext.containsKey(QueryVars.PYTHON_CONTROLLER_LOG_DIR)) {
      LOG.warn("Currently, logging is not supported for the python controller.");
      standardOutputRootWriteLocation = invokeContext.getQueryContext().get(QueryVars.PYTHON_CONTROLLER_LOG_DIR);
    }
//    standardOutputRootWriteLocation = "/home/jihoon/Projects/tajo/";
    standardOutputRootWriteLocation = "/Users/jihoonson/Projects/tajo/";
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
    fileName = fileName.endsWith(".py") ? fileName.substring(0, fileName.length()-3) : fileName;
    command[UDF_FILE_NAME] = fileName;
    command[UDF_FILE_PATH] = lastSeparator <= 0 ? "." : filePath.substring(0, lastSeparator - 1);
    command[UDF_NAME] = funcName;
    if (!invokeContext.getQueryContext().containsKey(QueryVars.PYTHON_SCRIPT_CODE_DIR)) {
      throw new IOException(TajoConf.ConfVars.PYTHON_CODE_DIR.keyname() + " must be set.");
    }
    String fileCachePath = invokeContext.getQueryContext().get(QueryVars.PYTHON_SCRIPT_CODE_DIR);
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

  private void setStreams() throws IOException {
    stdout = new DataInputStream(new BufferedInputStream(process.getInputStream()));
    outputHandler.bindTo(stdout);

    stdin = new DataOutputStream(new BufferedOutputStream(process.getOutputStream()));
    inputHandler.bindTo(stdin);

    stderr = new DataInputStream(new BufferedInputStream(process.getErrorStream()));
  }

  private void startThreads() {
//    stdinThread = new ProcessInputThread();
//    stdinThread.start();

//    stdoutThread = new ProcessOutputThread();
//    stdoutThread.start();

//    stderrThread = new ProcessErrorThread();
//    stderrThread.start();
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
      File controllerFile = File.createTempFile("controller", ".py");
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
    if (outputQueue == null) {
      throw new RuntimeException("Process has already been shut down.  No way to retrieve output for input: " + input);
    }

    try {
      if (input == null) {
        // When nothing is passed into the UDF the tuple
        // being sent is the full tuple for the relation.
        // We want it to be nothing (since that's what the user wrote).
        input = new VTuple(0);
      }

//      inputQueue.put(input);
      inputHandler.putNext(input);
      stdin.flush();
    } catch (Exception e) {
      throw new RuntimeException("Failed adding input to inputQueue", e);
    }
    Object o = null;
    try {
      if (outputQueue != null) {
//        o = outputQueue.take();
        o = outputHandler.getNext().get(0);
        if (o == NULL_OBJECT) {
          o = null;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Problem getting output", e);
    }

    if (o == ERROR_OUTPUT) {
      outputQueue = null;
      if (outerrThreadsError == null) {
        outerrThreadsError = new StreamingUDFException("python", "Problem with streaming udf.  Can't recreate exception.");
      }
      throw new RuntimeException(outerrThreadsError);
    }

    return (Datum) o;
  }

  /**
   * The thread which consumes input and feeds it to the the Process
   */
  class ProcessInputThread extends Thread {
    ProcessInputThread() {
      setDaemon(true);
    }

    public void run() {
      try {
        while (!isStopped) {
          Tuple inputTuple = inputQueue.poll(10, TimeUnit.MILLISECONDS);
          if (inputTuple != null) {
            inputHandler.putNext(inputTuple);
          }
          try {
            stdin.flush();
          } catch(Exception e) {
            return;
          }
        }
      } catch (InterruptedException e) {

      } catch (Exception e) {
        LOG.error(e);
      }
    }
  }

  private static final int WAIT_FOR_ERROR_LENGTH = 500;
  private static final int MAX_WAIT_FOR_ERROR_ATTEMPTS = 5;

  /**
   * The thread which consumes output from process
   */
  class ProcessOutputThread extends Thread {
    ProcessOutputThread() {
      setDaemon(true);
    }

    public void run() {
      Object o;
      try {

        o = outputHandler.getNext().get(0);
        while (!isStopped && o != OutputHandler.END_OF_OUTPUT) {
          if (o != null) {
            outputQueue.put(o);
          }
          else {
            outputQueue.put(NULL_OBJECT);
          }
          o = outputHandler.getNext().get(0);
        }
      } catch (IOException e) {
        // EOF
      } catch(Exception e) {
        if (outputQueue != null) {
          try {
            // Give error thread a chance to check the standard error output
            // for an exception message.
            int attempt = 0;
            while (stderrThread.isAlive() && attempt < MAX_WAIT_FOR_ERROR_ATTEMPTS) {
              Thread.sleep(WAIT_FOR_ERROR_LENGTH);
              attempt++;
            }
            // Only write this if no other error.  Don't want to overwrite
            // an error from the error thread.
            if (outerrThreadsError == null) {
              outerrThreadsError = new StreamingUDFException(
                  PYTHON_LANGUAGE, "Error deserializing output.  Please check that the declared outputSchema for function " +
                  invocationDesc.getName() + " matches the data type being returned.", e);
            }
            // TODO: Currently, errors occurred before executing an input are ignored.
            outputQueue.put(ERROR_OUTPUT); // Need to wake main thread.
          } catch(InterruptedException ie) {
            LOG.error(ie);
          }
        }
      }
    }
  }

  class ProcessErrorThread extends Thread {
    public ProcessErrorThread() {
      setDaemon(true);
    }

    public void run() {
      try {
        Integer lineNumber = null;
        StringBuffer error = new StringBuffer();
        String errInput;
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(stderr, Charsets.UTF_8));
        while (!isStopped && ((errInput = reader.readLine()) != null)) {
          // First line of error stream is usually the line number of error.
          // If its not a number just treat it as first line of error message.
          if (lineNumber == null) {
            try {
              lineNumber = Integer.valueOf(errInput);
            } catch (NumberFormatException nfe) {
              error.append(errInput + "\n");
            }
          } else {
            error.append(errInput + "\n");
          }
        }
        if (!isStopped) {
          outerrThreadsError = new StreamingUDFException(PYTHON_LANGUAGE, error.toString(), lineNumber);
          if (outputQueue != null) {
            // TODO: Currently, errors occurred before executing an input are ignored.
            outputQueue.put(ERROR_OUTPUT); // Need to wake main thread.
          }
          if (stderr != null) {
            stderr.close();
            stderr = null;
          }
        }
      } catch (IOException e) {
        LOG.info("Process Ended", e);
      } catch (Exception e) {
        LOG.error("standard error problem", e);
      }
    }
  }

  class ProcessKiller extends Thread {
    public ProcessKiller() {
      setDaemon(true);
    }
    public void run() {
      try {
        shutdown();
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
