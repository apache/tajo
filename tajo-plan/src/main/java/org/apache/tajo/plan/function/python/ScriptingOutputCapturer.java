package org.apache.tajo.plan.function.python;

import com.google.common.base.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.util.TUtil;

import java.io.*;
import java.util.Map;
import java.util.UUID;

public class ScriptingOutputCapturer {
  private static Log log = LogFactory.getLog(ScriptingOutputCapturer.class);

  private static Map<String, String> outputFileNames = TUtil.newHashMap();
  //Unique ID for this run to ensure udf output files aren't corrupted from previous runs.
  private static String runId = UUID.randomUUID().toString();

  //Illustrate will set the static flag telling udf to start capturing its output.  It's up to each
  //instance to react to it and set its own flag.
  private static boolean captureOutput = false;
  private boolean instancedCapturingOutput = false;

  private FunctionDesc functionDesc;
  private OverridableConf queryContext;

  public ScriptingOutputCapturer(OverridableConf queryContext, FunctionDesc functionDesc) {
    this.queryContext = queryContext;
    this.functionDesc = functionDesc;
  }

  public String getStandardOutputRootWriteLocation() throws IOException {
//    Configuration conf = UDFContext.getUDFContext().getJobConf();
//
//    String jobId = conf.get(MRConfiguration.JOB_ID);
//    String taskId = conf.get(MRConfiguration.TASK_ID);
//    String hadoopLogDir = System.getProperty("yarn.app.container.log.dir");
//    if (hadoopLogDir == null) {
//      hadoopLogDir = conf.get("yarn.app.container.log.dir");
//    }
//    if (hadoopLogDir == null) {
//      hadoopLogDir = System.getProperty("hadoop.log.dir");
//    }
//    if (hadoopLogDir == null) {
//      hadoopLogDir = conf.get("hadoop.log.dir");
//    }
//
//    String tmpDir = conf.get("hadoop.tmp.dir");
//    boolean fallbackToTmp = (hadoopLogDir == null);
//    if (!fallbackToTmp) {
//      try {
//        if (!(new File(hadoopLogDir).canWrite())) {
//          fallbackToTmp = true;
//        }
//      }
//      catch (SecurityException e) {
//        fallbackToTmp = true;
//      }
//      finally {
//        if (fallbackToTmp)
//          log.warn(String.format("Insufficient permission to write into %s. Change path to: %s", hadoopLogDir, tmpDir));
//      }
//    }
//    if (fallbackToTmp) {
//      hadoopLogDir = tmpDir;
//    }
//    log.debug("JobId: " + jobId);
//    log.debug("TaskId: " + taskId);
//    log.debug("hadoopLogDir: " + hadoopLogDir);
//
//    if (execType.isLocal() || conf.getBoolean(PigImplConstants.CONVERTED_TO_FETCH, false)) {
//      String logDir = System.getProperty("pig.udf.scripting.log.dir");
//      if (logDir == null)
//        logDir = ".";
//      return logDir + "/" + (taskId == null ? "" : (taskId + "_"));
//    } else {
//      String taskLogDir = getTaskLogDir(jobId, taskId, hadoopLogDir);
//      return taskLogDir + "/";
//    }
    return null;
  }

  public String getTaskLogDir(String jobId, String taskId, String hadoopLogDir) throws IOException {
    String taskLogDir = null;
//    String defaultUserLogDir = hadoopLogDir + File.separator + "userlogs";
//
//    if ( new File(defaultUserLogDir + File.separator + jobId).exists() ) {
//      taskLogDir = defaultUserLogDir + File.separator + jobId + File.separator + taskId;
//    } else if ( new File(defaultUserLogDir + File.separator + taskId).exists() ) {
//      taskLogDir = defaultUserLogDir + File.separator + taskId;
//    } else if ( new File(defaultUserLogDir).exists() ){
//      taskLogDir = defaultUserLogDir;
//    } else {
//      taskLogDir = hadoopLogDir + File.separator + "udfOutput";
//      File dir = new File(taskLogDir);
//      dir.mkdirs();
//      if (!dir.exists()) {
//        throw new IOException("Could not create directory: " + taskLogDir);
//      }
//    }
    return taskLogDir;
  }

  public static void startCapturingOutput() {
    ScriptingOutputCapturer.captureOutput = true;
  }

  public static Map<String, String> getUdfOutput() throws IOException {
    Map<String, String> udfFuncNameToOutput = TUtil.newHashMap();
    for (Map.Entry<String, String> funcToOutputFileName : outputFileNames.entrySet()) {
      StringBuffer udfOutput = new StringBuffer();
      FileInputStream fis = new FileInputStream(funcToOutputFileName.getValue());
      Reader fr = new InputStreamReader(fis, Charsets.UTF_8);
      BufferedReader br = new BufferedReader(fr);

      try {
        String line = br.readLine();
        while (line != null) {
          udfOutput.append("\t" + line + "\n");
          line = br.readLine();
        }
      } finally {
        br.close();
      }
      udfFuncNameToOutput.put(funcToOutputFileName.getKey(), udfOutput.toString());
    }
    return udfFuncNameToOutput;
  }

  public void registerOutputLocation(String functionName, String fileName) {
    outputFileNames.put(functionName, fileName);
  }

  public static String getRunId() {
    return runId;
  }

  public static boolean isClassCapturingOutput() {
    return ScriptingOutputCapturer.captureOutput;
  }

  public boolean isInstanceCapturingOutput() {
    return this.instancedCapturingOutput;
  }

  public void setInstanceCapturingOutput(boolean instanceCapturingOutput) {
    this.instancedCapturingOutput = instanceCapturingOutput;
  }
}
