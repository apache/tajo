package org.apache.tajo.plan.expr;

import com.google.common.base.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.plan.function.python.ScriptingOutputCapturer;
import org.apache.tajo.plan.function.stream.StreamingUDFException;
import org.apache.tajo.storage.Tuple;

import java.io.*;
import java.util.concurrent.BlockingQueue;

public class PythonFunctionInvoke2 extends FunctionInvoke {

  private static final Log log = LogFactory.getLog(PythonFunctionInvoke2.class);

  private static final String PYTHON_CONTROLLER_JAR_PATH = "/python/controller.py"; //Relative to root of tajo jar.
  private static final String PYTHON_PIG_UTIL_PATH = "/python/tajo_util.py"; //Relative to root of tajo jar.

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

  private String language;
  private String filePath;
  private String funcName;

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

  public static final String TURN_ON_OUTPUT_CAPTURING = "TURN_ON_OUTPUT_CAPTURING";

  public PythonFunctionInvoke2(FunctionDesc functionDesc) {
    super(functionDesc);
  }

  @Override
  public void init(OverridableConf queryContext, FunctionEval.ParamType[] paramTypes) {

  }

  @Override
  public Datum eval(Tuple tuple) {
    return null;
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
        log.debug("Starting PIT");
        while (true) {
          Tuple inputTuple = inputQueue.take();
          inputHandler.putNext(inputTuple);
          try {
            stdin.flush();
          } catch(Exception e) {
            return;
          }
        }
      } catch (Exception e) {
        log.error(e);
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
        log.debug("Starting POT");
        //StreamUDFToPig wraps object in single element tuple
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
                  language, "Error deserializing output.  Please check that the declared outputSchema for function " +
                  funcName + " matches the data type being returned.", e);
            }
            outputQueue.put(ERROR_OUTPUT); //Need to wake main thread.
          } catch(InterruptedException ie) {
            log.error(ie);
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
        log.debug("Starting PET");
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
        outerrThreadsError = new StreamingUDFException(language, error.toString(), lineNumber);
        if (outputQueue != null) {
          outputQueue.put(ERROR_OUTPUT); //Need to wake main thread.
        }
        if (stderr != null) {
          stderr.close();
          stderr = null;
        }
      } catch (IOException e) {
        log.debug("Process Ended", e);
      } catch (Exception e) {
        log.error("standard error problem", e);
      }
    }
  }

  public class ProcessKiller implements Runnable {
    public void run() {
      process.destroy();
    }
  }
}
