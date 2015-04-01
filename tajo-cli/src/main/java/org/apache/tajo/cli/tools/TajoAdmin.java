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

package org.apache.tajo.cli.tools;

import com.google.protobuf.ServiceException;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.client.TajoClientUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ha.HAServiceUtil;
import org.apache.tajo.ipc.ClientProtos.BriefQueryInfo;
import org.apache.tajo.ipc.ClientProtos.WorkerResourceInfo;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.TajoIdUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class TajoAdmin {
  private static final org.apache.commons.cli.Options options;
  private static DecimalFormat decimalF = new DecimalFormat("###.0");
  private enum WorkerStatus {
    RUNNING,
    LOST,
    DECOMMISSIONED
  }

  final static String DASHLINE_LEN5 = "-----";
  final static String DASHLINE_LEN10 = "----------";
  final static String DASHLINE_LEN12 = "------------";
  final static String DASHLINE_LEN25 = "-------------------------";
  final static String DATE_FORMAT  = "yyyy-MM-dd HH:mm:ss";

  static {
    options = new Options();
    options.addOption("h", "host", true, "Tajo server host");
    options.addOption("p", "port", true, "Tajo server port");
    options.addOption("list", null, false, "Show Tajo query list");
    options.addOption("cluster", null, false, "Show Cluster Info");
    options.addOption("showmasters", null, false, "gets list of tajomasters in the cluster");
    options.addOption("desc", null, false, "Show Query Description");
    options.addOption("kill", null, true, "Kill a running query");
  }

  private TajoConf tajoConf;
  private TajoClient tajoClient;
  private Writer writer;

  public TajoAdmin(TajoConf tajoConf, Writer writer) {
    this(tajoConf, writer, null);
  }

  public TajoAdmin(TajoConf tajoConf, Writer writer, TajoClient tajoClient) {
    this.tajoConf = tajoConf;
    this.writer = writer;
    this.tajoClient = tajoClient;
  }

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "admin [options]", options );
  }

  public void runCommand(String[] args) throws Exception {
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    String param = "";
    int cmdType = 0;

    String hostName = null;
    Integer port = null;
    if (cmd.hasOption("h")) {
      hostName = cmd.getOptionValue("h");
    }
    if (cmd.hasOption("p")) {
      port = Integer.parseInt(cmd.getOptionValue("p"));
    }

    String queryId = null;

    if (cmd.hasOption("list")) {
      cmdType = 1;
    } else if (cmd.hasOption("desc")) {
      cmdType = 2;
    } else if (cmd.hasOption("cluster")) {
      cmdType = 3;
    } else if (cmd.hasOption("kill")) {
      cmdType = 4;
      queryId = cmd.getOptionValue("kill");
    } else if (cmd.hasOption("showmasters")) {
      cmdType = 5;
    }

    // if there is no "-h" option,
    if(hostName == null) {
      if (tajoConf.getVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS) != null) {
        // it checks if the client service address is given in configuration and distributed mode.
        // if so, it sets entryAddr.
        hostName = tajoConf.getVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS).split(":")[0];
      }
    }
    if (port == null) {
      if (tajoConf.getVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS) != null) {
        // it checks if the client service address is given in configuration and distributed mode.
        // if so, it sets entryAddr.
        port = Integer.parseInt(tajoConf.getVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS).split(":")[1]);
      }
    }

    if (cmdType == 0) {
      printUsage();
      return;
    }


    if ((hostName == null) ^ (port == null)) {
      System.err.println("ERROR: cannot find valid Tajo server address");
      return;
    } else if (hostName != null && port != null) {
      tajoConf.setVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS, hostName + ":" + port);
      tajoClient = new TajoClientImpl(ServiceTrackerFactory.get(tajoConf));
    } else if (hostName == null && port == null) {
      tajoClient = new TajoClientImpl(ServiceTrackerFactory.get(tajoConf));
    }

    switch (cmdType) {
      case 1:
        processList(writer);
        break;
      case 2:
        processDesc(writer);
        break;
      case 3:
        processCluster(writer);
        break;
      case 4:
        processKill(writer, queryId);
        break;
      case 5:
        processMasters(writer);
        break;
      default:
        printUsage();
        break;
    }

    writer.flush();
  }

  private void processDesc(Writer writer) throws ParseException, IOException,
      ServiceException, SQLException {

    List<BriefQueryInfo> queryList = tajoClient.getRunningQueryList();
    SimpleDateFormat df = new SimpleDateFormat(DATE_FORMAT);
    int id = 1;
    for (BriefQueryInfo queryInfo : queryList) {
        String queryId = String.format("q_%s_%04d",
                                       queryInfo.getQueryId().getId(),
                                       queryInfo.getQueryId().getSeq());

        writer.write("Id: " + id);
        writer.write("\n");
        id++;
        writer.write("Query Id: " + queryId);
        writer.write("\n");
        writer.write("Started Time: " + df.format(queryInfo.getStartTime()));
        writer.write("\n");

        writer.write("Query State: " + queryInfo.getState().name());
        writer.write("\n");
        long end = queryInfo.getFinishTime();
        long start = queryInfo.getStartTime();
        String executionTime = decimalF.format((end-start) / 1000) + " sec";
        if (TajoClientUtil.isQueryComplete(queryInfo.getState())) {
          writer.write("Finished Time: " + df.format(queryInfo.getFinishTime()));
          writer.write("\n");
        }
        writer.write("Execution Time: " + executionTime);
        writer.write("\n");
        writer.write("Query Progress: " + queryInfo.getProgress());
        writer.write("\n");
        writer.write("Query Statement:");
        writer.write("\n");
        writer.write(queryInfo.getQuery());
        writer.write("\n");
        writer.write("\n");
    }
  }

  private void processCluster(Writer writer) throws ParseException, IOException,
      ServiceException, SQLException {

    List<WorkerResourceInfo> workerList = tajoClient.getClusterInfo();

    int runningQueryMasterTasks = 0;

    List<WorkerResourceInfo> liveWorkers = new ArrayList<WorkerResourceInfo>();
    List<WorkerResourceInfo> deadWorkers = new ArrayList<WorkerResourceInfo>();
    List<WorkerResourceInfo> decommissionWorkers = new ArrayList<WorkerResourceInfo>();

    List<WorkerResourceInfo> liveQueryMasters = new ArrayList<WorkerResourceInfo>();
    List<WorkerResourceInfo> deadQueryMasters = new ArrayList<WorkerResourceInfo>();

    for (WorkerResourceInfo eachWorker : workerList) {
      if(eachWorker.getWorkerStatus().equals(WorkerStatus.RUNNING.toString())) {
        liveQueryMasters.add(eachWorker);
        liveWorkers.add(eachWorker);
        runningQueryMasterTasks += eachWorker.getNumQueryMasterTasks();
      } else if(eachWorker.getWorkerStatus().equals(WorkerStatus.LOST.toString())) {
        deadQueryMasters.add(eachWorker);
        deadWorkers.add(eachWorker);
      } else if(eachWorker.getWorkerStatus().equals(WorkerStatus.DECOMMISSIONED.toString())) {
        decommissionWorkers.add(eachWorker);
      }
    }

    String fmtInfo = "%1$-5s %2$-5s %3$-5s%n";
    String infoLine = String.format(fmtInfo, "Live", "Dead", "Tasks");

    writer.write("Query Master\n");
    writer.write("============\n\n");
    writer.write(infoLine);
    String line = String.format(fmtInfo, DASHLINE_LEN5, DASHLINE_LEN5, DASHLINE_LEN5);
    writer.write(line);

    line = String.format(fmtInfo, liveQueryMasters.size(),
                         deadQueryMasters.size(), runningQueryMasterTasks);
    writer.write(line);
    writer.write("\n");

    writer.write("Live QueryMasters\n");
    writer.write("=================\n\n");

    if (liveQueryMasters.isEmpty()) {
      writer.write("No Live QueryMasters\n");
    } else {
      String fmtQueryMasterLine = "%1$-25s %2$-5s %3$-5s %4$-10s %5$-10s%n";
      line = String.format(fmtQueryMasterLine, "QueryMaster", "Port", "Query",
                           "Heap", "Status");
      writer.write(line);
      line = String.format(fmtQueryMasterLine, DASHLINE_LEN25, DASHLINE_LEN5,
              DASHLINE_LEN5, DASHLINE_LEN10, DASHLINE_LEN10);
      writer.write(line);
      for (WorkerResourceInfo queryMaster : liveQueryMasters) {
        TajoProtos.WorkerConnectionInfoProto connInfo = queryMaster.getConnectionInfo();
        String queryMasterHost = String.format("%s:%d", connInfo.getHost(), connInfo.getQueryMasterPort());
        String heap = String.format("%d MB", queryMaster.getMaxHeap() / 1024 / 1024);
        line = String.format(fmtQueryMasterLine,
            queryMasterHost,
            connInfo.getClientPort(),
            queryMaster.getNumQueryMasterTasks(),
            heap,
            queryMaster.getWorkerStatus());
        writer.write(line);
      }

      writer.write("\n\n");
    }

    if (!deadQueryMasters.isEmpty()) {
      writer.write("Dead QueryMasters\n");
      writer.write("=================\n\n");

      String fmtQueryMasterLine = "%1$-25s %2$-5s %3$-10s%n";
      line = String.format(fmtQueryMasterLine, "QueryMaster", "Port", "Status");
      writer.write(line);
      line = String.format(fmtQueryMasterLine, DASHLINE_LEN25, DASHLINE_LEN5, DASHLINE_LEN10);
      writer.write(line);

      for (WorkerResourceInfo queryMaster : deadQueryMasters) {
        TajoProtos.WorkerConnectionInfoProto connInfo = queryMaster.getConnectionInfo();
        String queryMasterHost = String.format("%s:%d", connInfo.getHost(), connInfo.getQueryMasterPort());
        line = String.format(fmtQueryMasterLine,
            queryMasterHost,
            connInfo.getClientPort(),
            queryMaster.getWorkerStatus());
        writer.write(line);
      }

      writer.write("\n\n");
    }

    writer.write("Worker\n");
    writer.write("======\n\n");

    String fmtWorkerInfo = "%1$-5s %2$-5s%n";
    String workerInfoLine = String.format(fmtWorkerInfo, "Live", "Dead");
    writer.write(workerInfoLine);
    line = String.format(fmtWorkerInfo, DASHLINE_LEN5, DASHLINE_LEN5);
    writer.write(line);

    line = String.format(fmtWorkerInfo, liveWorkers.size(), deadWorkers.size());
    writer.write(line);
    writer.write("\n");

    writer.write("Live Workers\n");
    writer.write("============\n\n");
    if(liveWorkers.isEmpty()) {
      writer.write("No Live Workers\n\n");
    } else {
      writeWorkerInfo(writer, liveWorkers);
    }

    writer.write("Dead Workers\n");
    writer.write("============\n\n");
    if(deadWorkers.isEmpty()) {
      writer.write("No Dead Workers\n\n");
    } else {
      writeWorkerInfo(writer, deadWorkers);
    }
  }

  private void writeWorkerInfo(Writer writer, List<WorkerResourceInfo> workers) throws ParseException,
      IOException, ServiceException, SQLException {
    String fmtWorkerLine = "%1$-25s %2$-5s %3$-5s %4$-10s %5$-10s %6$-12s %7$-10s%n";
    String line = String.format(fmtWorkerLine,
        "Worker", "Port", "Tasks",
        "Mem", "Disk",
        "Heap", "Status");
    writer.write(line);
    line = String.format(fmtWorkerLine,
        DASHLINE_LEN25, DASHLINE_LEN5, DASHLINE_LEN5,
        DASHLINE_LEN10, DASHLINE_LEN10,
        DASHLINE_LEN12, DASHLINE_LEN10);
    writer.write(line);

    for (WorkerResourceInfo worker : workers) {
      TajoProtos.WorkerConnectionInfoProto connInfo = worker.getConnectionInfo();
      String workerHost = String.format("%s:%d", connInfo.getHost(), connInfo.getPeerRpcPort());
      String mem = String.format("%d/%d", worker.getUsedMemoryMB(),
          worker.getMemoryMB());
      String disk = String.format("%.2f/%.2f", worker.getUsedDiskSlots(),
          worker.getDiskSlots());
      String heap = String.format("%d/%d MB", worker.getFreeHeap()/1024/1024,
          worker.getMaxHeap()/1024/1024);

      line = String.format(fmtWorkerLine, workerHost,
          connInfo.getPullServerPort(),
          worker.getNumRunningTasks(),
          mem, disk, heap, worker.getWorkerStatus());
      writer.write(line);
    }
    writer.write("\n\n");
  }

  private void processList(Writer writer) throws ParseException, IOException,
      ServiceException, SQLException {

    List<BriefQueryInfo> queryList = tajoClient.getRunningQueryList();
    SimpleDateFormat df = new SimpleDateFormat(DATE_FORMAT);
    StringBuilder builder = new StringBuilder();

    /* print title */
    builder.append(StringUtils.rightPad("QueryId", 21));
    builder.append(StringUtils.rightPad("State", 20));
    builder.append(StringUtils.rightPad("StartTime", 20));
    builder.append(StringUtils.rightPad("Query", 30)).append("\n");

    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 20), 21));
    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 19), 20));
    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 19), 20));
    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 29), 30)).append("\n");
    writer.write(builder.toString());

    builder = new StringBuilder();
    for (BriefQueryInfo queryInfo : queryList) {
      builder.append(StringUtils.rightPad(new QueryId(queryInfo.getQueryId()).toString(), 21));
      builder.append(StringUtils.rightPad(queryInfo.getState().name(), 20));
      builder.append(StringUtils.rightPad(df.format(queryInfo.getStartTime()), 20));
      builder.append(StringUtils.abbreviate(queryInfo.getQuery(), 30)).append("\n");
    }
    writer.write(builder.toString());
  }

  public void processKill(Writer writer, String queryIdStr)
      throws IOException, ServiceException {
    QueryStatus status = tajoClient.killQuery(TajoIdUtils.parseQueryId(queryIdStr));
    if (status.getState() == TajoProtos.QueryState.QUERY_KILLED) {
      writer.write(queryIdStr + " is killed successfully.\n");
    } else if (status.getState() == TajoProtos.QueryState.QUERY_KILL_WAIT) {
      writer.write(queryIdStr + " will be finished after a while.\n");
    } else {
      writer.write("ERROR:" + status.getErrorMessage());
    }
  }

  private void processMasters(Writer writer) throws ParseException, IOException,
      ServiceException, SQLException {

    if (tajoConf.getBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE)) {

      List<String> list = HAServiceUtil.getMasters(tajoConf);
      int i = 0;
      for (String master : list) {
        if (i > 0) {
          writer.write(" ");
        }
        writer.write(master);
        i++;
      }
      writer.write("\n");
    } else {
      String confMasterServiceAddr = tajoConf.getVar(TajoConf.ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS);
      InetSocketAddress masterAddress = NetUtils.createSocketAddr(confMasterServiceAddr);
      writer.write(masterAddress.getHostName());
      writer.write("\n");
    }
  }

  public static void main(String [] args) throws Exception {
    TajoConf conf = new TajoConf();

    Writer writer = new PrintWriter(System.out);
    try {
      TajoAdmin admin = new TajoAdmin(conf, writer);
      admin.runCommand(args);
    } finally {
      writer.close();
      System.exit(0);
    }
  }
}
