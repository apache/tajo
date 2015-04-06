/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tajo.plan.expr;

import com.google.common.base.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.function.FunctionSignature;
import org.apache.tajo.function.PythonInvocationDesc;
import org.apache.tajo.plan.function.python.JythonUtils;
import org.apache.tajo.plan.function.python.ScriptingOutputCapturer;
import org.apache.tajo.plan.function.stream.*;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.io.*;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class PythonFunctionInvoke2 extends FunctionInvoke {

  private static final Log LOG = LogFactory.getLog(PythonFunctionInvoke2.class);

  private static final String PYTHON_CONTROLLER_JAR_PATH = "/python/controller.py"; //Relative to root of tajo jar.
  private static final String PYTHON_TAJO_UTIL_PATH = "/python/tajo_util.py"; //Relative to root of tajo jar.

  //Indexes for arguments being passed to external process
  private static final int UDF_LANGUAGE = 0;
  private static final int PATH_TO_CONTROLLER_FILE = 1;
  private static final int UDF_FILE_NAME = 2; //Name of file where UDF function is defined
  private static final int UDF_FILE_PATH = 3; //Path to directory containing file where UDF function is defined
  private static final int UDF_NAME = 4; //Name of UDF function being called.
  private static final int PATH_TO_FILE_CACHE = 5; //Directory where required files (like tajo_util) are cached on cluster nodes.
  private static final int STD_OUT_OUTPUT_PATH = 6; //File for output from when user writes to standard output.
  private static final int STD_ERR_OUTPUT_PATH = 7; //File for output from when user writes to standard error.
  private static final int CONTROLLER_LOG_FILE_PATH = 8; //Controller log file logs progress through the controller script not user code.
  private static final int IS_ILLUSTRATE = 9; //Controller captures output differently in illustrate vs running.

  private ScriptingOutputCapturer soc;

  private Process process; // Handle to the externwlgns1441
  // al process
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
  private static final Object NULL_OBJECT = new Object(); //BlockingQueue can't have null.  Use place holder object instead.

  private volatile StreamingUDFException outerrThreadsError;

  private OverridableConf queryContext = null;

  private FunctionSignature functionSignature;
  private PythonInvocationDesc invocationDesc;
  private Schema inSchema;
  private Schema outSchema;
  private int [] projectionCols;

  private CSVLineSerDe lineSerDe = new CSVLineSerDe();
  private TableMeta pipeMeta;

  public static final String TURN_ON_OUTPUT_CAPTURING = "TURN_ON_OUTPUT_CAPTURING";

  public PythonFunctionInvoke2(FunctionDesc functionDesc) {
    super(functionDesc);
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

  @Override
  public void init(OverridableConf queryContext, FunctionEval.ParamType[] paramTypes) throws IOException {
    this.queryContext = queryContext;
    this.inputQueue = new ArrayBlockingQueue<Tuple>(1);
    this.outputQueue = new ArrayBlockingQueue<Object>(2);
    this.soc = new ScriptingOutputCapturer(queryContext, functionDesc);
    startUdfController();
    createInputHandlers();
    setStreams();
    startThreads();
  }

  private StreamingCommand startUdfController() throws IOException {
    StreamingCommand sc = new StreamingCommand(buildCommand());
    ProcessBuilder processBuilder = StreamingUtil.createProcess(queryContext, sc);
    process = processBuilder.start();
    LOG.info("process started.");

    Runtime.getRuntime().addShutdownHook(new Thread(new ProcessKiller()));
    return sc;
  }

  private String[] buildCommand() throws IOException {
    String[] command = new String[10];

//    String jarPath = conf.get("mapreduce.job.jar");
//    if (jarPath == null) {
//      jarPath = conf.get(MRConfiguration.JAR);
//    }
//    String jobDir;
//    if (jarPath != null) {
//      jobDir = new File(jarPath).getParent();
//    } else {
//      jobDir = "";
//    }

//    String standardOutputRootWriteLocation = soc.getStandardOutputRootWriteLocation();
//    String standardOutputRootWriteLocation = System.getProperty("tajo.log.dir");
    // TODO
//    String standardOutputRootWriteLocation = "/Users/jihoonson/Projects/tajo/";
    String standardOutputRootWriteLocation = "/home/jihoon/Projects/tajo/";
    String controllerLogFileName, outFileName, errOutFileName;

//    if (execType.isLocal()) {
//      controllerLogFileName = standardOutputRootWriteLocation + funcName + "_python.log";
//      outFileName = standardOutputRootWriteLocation + "cpython_" + funcName + "_" + ScriptingOutputCapturer.getRunId() + ".out";
//      errOutFileName = standardOutputRootWriteLocation + "cpython_" + funcName + "_" + ScriptingOutputCapturer.getRunId() + ".err";
//    } else {
    String funcName = invocationDesc.getName();
    String filePath = invocationDesc.getPath();

      controllerLogFileName = standardOutputRootWriteLocation + funcName + "_python.log";
      outFileName = standardOutputRootWriteLocation + funcName + ".out";
      errOutFileName = standardOutputRootWriteLocation + funcName + ".err";
//    }

    soc.registerOutputLocation(funcName, outFileName);

    command[UDF_LANGUAGE] = "python";
    command[PATH_TO_CONTROLLER_FILE] = getControllerPath();
    int lastSeparator = filePath.lastIndexOf(File.separator) + 1;
    String fileName = filePath.substring(lastSeparator);
    fileName = fileName.endsWith(".py") ? fileName.substring(0, fileName.length()-3) : fileName;
    command[UDF_FILE_NAME] = fileName;
    command[UDF_FILE_PATH] = lastSeparator <= 0 ?
        "." :
        filePath.substring(0, lastSeparator - 1);
    command[UDF_NAME] = funcName;
    // TODO
    String fileCachePath = filePath.substring(0, lastSeparator);
//    command[PATH_TO_FILE_CACHE] = "'" + fileCachePath + "'";
//    command[PATH_TO_FILE_CACHE] = "'" + "/Users/jihoonson/Projects/tajo/tajo-core/src/test/resources/python/" + "'";
    command[PATH_TO_FILE_CACHE] = "'" + "/home/jihoon/Projects/tajo/tajo-core/src/test/resources/python/" + "'";
    command[STD_OUT_OUTPUT_PATH] = outFileName;
    command[STD_ERR_OUTPUT_PATH] = errOutFileName;
    command[CONTROLLER_LOG_FILE_PATH] = controllerLogFileName;
    command[IS_ILLUSTRATE] = "false";

//    ensureUserFileAvailable(command, fileCachePath);

    for (String cmd : command) {
      LOG.info(cmd);
    }

    return command;
  }

  /**
   * Need to make sure the user's file is available. If jar hasn't been
   * exploded, just copy the udf file to its path relative to the controller
   * file and update file cache path appropriately.
   */
  private void ensureUserFileAvailable(String[] command, String fileCachePath)
      throws IOException {

    File userUdfFile = new File(fileCachePath + command[UDF_FILE_NAME]);
    if (!userUdfFile.exists()) {
      String filePath = invocationDesc.getPath();
      String absolutePath = filePath.startsWith("/") ? filePath : "/" + filePath;
      absolutePath = absolutePath.replaceAll(":", "");
      String controllerDir = new File(command[PATH_TO_CONTROLLER_FILE]).getParent();
      String userUdfPath = controllerDir + absolutePath + getUserFileExtension();
      userUdfFile = new File(userUdfPath);
      userUdfFile.deleteOnExit();
      userUdfFile.getParentFile().mkdirs();
      if (userUdfFile.exists()) {
        userUdfFile.delete();
        if (!userUdfFile.createNewFile()) {
          throw new IOException("Unable to create file: " + userUdfFile.getAbsolutePath());
        }
      }
      InputStream udfFileStream = this.getClass().getResourceAsStream(
          absolutePath + getUserFileExtension());
      command[PATH_TO_FILE_CACHE] = "\"" + userUdfFile.getParentFile().getAbsolutePath()
          + "\"";

      try {
        FileUtils.copyInputStreamToFile(udfFileStream, userUdfFile);
      }
      catch (Exception e) {
        throw new IOException("Unable to copy user udf file: " + userUdfFile.getName(), e);
      }
      finally {
        udfFileStream.close();
      }
    }
  }

  private String getUserFileExtension() {
    return ".py";
  }

  private void createInputHandlers() {
    TextLineSerializer serializer = lineSerDe.createSerializer(inSchema, pipeMeta);
    serializer.init();
    this.inputHandler = new StreamingUDFInputHandler(serializer);
    TextLineDeserializer deserializer = lineSerDe.createDeserializer(outSchema, pipeMeta, projectionCols);
    deserializer.init();
    this.outputHandler = new StreamingUDFOutputHandler(deserializer);
  }

  private void setStreams() throws IOException {
    stdout = new DataInputStream(new BufferedInputStream(process
        .getInputStream()));
    outputHandler.bindTo("", stdout,
        0, Long.MAX_VALUE);

    stdin = new DataOutputStream(new BufferedOutputStream(process
        .getOutputStream()));
    inputHandler.bindTo(stdin);

    stderr = new DataInputStream(new BufferedInputStream(process
        .getErrorStream()));
  }

  private void startThreads() {
    stdinThread = new ProcessInputThread();
    stdinThread.start();

    stdoutThread = new ProcessOutputThread();
    stdoutThread.start();

    stderrThread = new ProcessErrorThread();
    stderrThread.start();
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
      File tajoUtilFile = new File(controllerFile.getParent() + "/tajo_util.py");
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

  @Override
  public Datum eval(Tuple tuple) {
    return getOutput(tuple);
  }

  private Datum getOutput(Tuple input) {
    if (outputQueue == null) {
      throw new RuntimeException("Process has already been shut down.  No way to retrieve output for input: " + input);
    }

//    if (ScriptingOutputCapturer.isClassCapturingOutput() &&
//        !soc.isInstanceCapturingOutput()) {
//      Tuple t = TupleFactory.getInstance().newTuple(TURN_ON_OUTPUT_CAPTURING);
//      try {
//        inputQueue.put(t);
//      } catch (InterruptedException e) {
//        throw new RuntimeException("Failed adding capture input flag to inputQueue");
//      }
//      soc.setInstanceCapturingOutput(true);
//    }

    try {
//      if (this.inSchema == null || this.inSchema.size() == 0) {
      if (input == null) {
        //When nothing is passed into the UDF the tuple
        //being sent is the full tuple for the relation.
        //We want it to be nothing (since that's what the user wrote).
//        input = TupleFactory.getInstance().newTuple(0);
        input = new VTuple(0);
      }
      LOG.info("input: " + input);
      inputQueue.put(input);
    } catch (Exception e) {
      throw new RuntimeException("Failed adding input to inputQueue", e);
    }
    Object o = null;
    try {
      if (outputQueue != null) {
        o = outputQueue.take();
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

//    Datum out = JythonUtils.objectToDatum(outSchema.getColumn(0).getDataType(), o);
//    LOG.info("out: " + out);
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
        LOG.info("Starting PIT");
        while (true) {
          Tuple inputTuple = inputQueue.take();
          LOG.info("PIT: " + inputTuple);
          inputHandler.putNext(inputTuple);
          try {
            stdin.flush();
            LOG.info("PIT flushed");
          } catch(Exception e) {
            return;
          }
        }
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
      Object o = null;
      try{
        LOG.info("Starting POT");

        o = outputHandler.getNext().get(0);
        while (o != OutputHandler.END_OF_OUTPUT) {
          if (o != null)
            outputQueue.put(o);
          else
            outputQueue.put(NULL_OBJECT);
          o = outputHandler.getNext().get(0);
        }
      } catch(Exception e) {
        if (outputQueue != null) {
          try {
            //Give error thread a chance to check the standard error output
            //for an exception message.
            int attempt = 0;
            while (stderrThread.isAlive() && attempt < MAX_WAIT_FOR_ERROR_ATTEMPTS) {
              Thread.sleep(WAIT_FOR_ERROR_LENGTH);
              attempt++;
            }
            //Only write this if no other error.  Don't want to overwrite
            //an error from the error thread.
            if (outerrThreadsError == null) {
              outerrThreadsError = new StreamingUDFException(
                  "python", "Error deserializing output.  Please check that the declared outputSchema for function " +
                  invocationDesc.getName() + " matches the data type being returned.", e);
            }
            outputQueue.put(ERROR_OUTPUT); //Need to wake main thread.
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
        LOG.info("Starting PET");
        Integer lineNumber = null;
        StringBuffer error = new StringBuffer();
        String errInput;
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(stderr, Charsets.UTF_8));
        while ((errInput = reader.readLine()) != null) {
          //First line of error stream is usually the line number of error.
          //If its not a number just treat it as first line of error message.
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
        outerrThreadsError = new StreamingUDFException("python", error.toString(), lineNumber);
        if (outputQueue != null) {
          outputQueue.put(ERROR_OUTPUT); //Need to wake main thread.
        }
        if (stderr != null) {
          stderr.close();
          stderr = null;
        }
      } catch (IOException e) {
        LOG.info("Process Ended", e);
      } catch (Exception e) {
        LOG.error("standard error problem", e);
      }
    }
  }

  public class ProcessKiller implements Runnable {
    public void run() {
      process.destroy();
      LOG.info("process destroyed.");
    }
  }
//
//  public static void main(String[] args) throws IOException {
//    String line;
//    Scanner scan = new Scanner(System.in);
//
//    Process process = Runtime.getRuntime ().exec("/bin/bash");
//    OutputStream stdin = process.getOutputStream();
//    InputStream stderr = process.getErrorStream();
//    InputStream stdout = process.getInputStream();
//
//    BufferedReader reader = new BufferedReader (new InputStreamReader(stdout));
//    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin));
//
//    while (scan.hasNext()) {
//      String input = scan.nextLine();
//      if (input.trim().equals("exit")) {
//        // Putting 'exit' amongst the echo --EOF--s below doesn't work.
//        writer.write("exit\n");
//      } else {
////        writer.write("((" + input + ") && echo --EOF--) || echo --EOF--\n");
//        writer.write("((" + input + ") && echo --EOF--)\n");
//      }
//      writer.flush();
//
//      line = reader.readLine();
//      while (line != null && ! line.trim().equals("--EOF--")) {
//        System.out.println ("Stdout: " + line);
//        line = reader.readLine();
//      }
//      if (line == null) {
//        break;
//      }
//    }
//  }
}
