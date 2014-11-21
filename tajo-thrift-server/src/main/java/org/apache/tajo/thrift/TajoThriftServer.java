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

package org.apache.tajo.thrift;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.thrift.generated.TajoThriftService;
import org.apache.tajo.util.NetUtils;

import java.io.PrintWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.util.Collection;

public class TajoThriftServer implements ThriftServerConstants {
  private static final Log LOG = LogFactory.getLog(TajoThriftServer.class);

  private TajoConf conf;

  private ThriftServerRunner serverRunner;

  private InfoHttpServer webServer;

  private ThriftServerContext context;

  private long startTime;

  private ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

  public TajoThriftServer(TajoConf conf) {
    this.conf = conf;
    context = new ThriftServerContext();
  }

  private static void printUsageAndExit(Options options, int exitCode)
      throws ExitCodeException {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setWidth(120);
    formatter.printHelp("Tajo Thrift-Server", null, options,
        "To start the Thrift server run 'bin/tajo-daemon.sh start thriftserver'\n" +
            "To shutdown the thrift server run 'bin/tajo-daemon.sh stop thriftserver' " +
            "or send a kill signal to the thrift server pid",
        true);
    System.exit(exitCode);
  }

  public ThriftServerContext getContext() {
    return context;
  }

  public TajoConf getConf() {
    return conf;
  }

  /**
   * Parse the command line options to set parameters the conf.
   */
  private void processOptions(final String[] args) throws Exception {
    Options options = new Options();
    options.addOption("b", BIND_OPTION, true, "Address to bind " +
        "the Thrift server to. [default: " + DEFAULT_BIND_ADDRESS + "]");
    options.addOption("p", PORT_OPTION, true, "Port to bind to [default: " + DEFAULT_LISTEN_PORT + "]");
    options.addOption("h", "help", false, "Print help information");

    options.addOption("m", MIN_WORKERS_OPTION, true, "The minimum number of worker threads");
    options.addOption("w", MAX_WORKERS_OPTION, true, "The maximum number of worker threads");
    options.addOption("q", MAX_QUEUE_SIZE_OPTION, true, "The maximum number of queued requests");
    options.addOption("k", KEEP_ALIVE_SEC_OPTION, true,
        "The amount of time in seconds to keep a thread alive when idle");

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("help")) {
      printUsageAndExit(options, 1);
    }

    // Make optional changes to the configuration based on command-line options
    optionToConf(cmd, MIN_WORKERS_OPTION, conf, TBoundedThreadPoolServer.MIN_WORKER_THREADS_CONF_KEY);
    optionToConf(cmd, MAX_WORKERS_OPTION, conf, TBoundedThreadPoolServer.MAX_WORKER_THREADS_CONF_KEY);
    optionToConf(cmd, MAX_QUEUE_SIZE_OPTION, conf, TBoundedThreadPoolServer.MAX_QUEUED_REQUESTS_CONF_KEY);
    optionToConf(cmd, KEEP_ALIVE_SEC_OPTION, conf, TBoundedThreadPoolServer.THREAD_KEEP_ALIVE_TIME_SEC_CONF_KEY);

    if (cmd.hasOption(BIND_OPTION)) {
      conf.set(SERVER_ADDRESS_CONF_KEY, cmd.getOptionValue(BIND_OPTION));
    }
    if (cmd.hasOption(PORT_OPTION)) {
      conf.set(SERVER_PORT_CONF_KEY, cmd.getOptionValue(PORT_OPTION));
    }
  }

  private static void optionToConf(CommandLine cmd, String option,
                                   Configuration conf, String destConfKey) {
    if (cmd.hasOption(option)) {
      String value = cmd.getOptionValue(option);
      LOG.info("Set configuration key:" + destConfKey + " value:" + value);
      conf.set(destConfKey, value);
    }
  }

  public void startServer(final String[] args, boolean blocking) throws Exception {
    processOptions(args);

    InetSocketAddress infoAddress =
        NetUtils.createSocketAddr(conf.get(INFO_SERVER_ADDRESS_CONF_KEY, INFO_SERVER_DEFAULT_ADDRESS));
    webServer = InfoHttpServer.getInstance(this ,"thrift", infoAddress.getHostName(), infoAddress.getPort(),
        true, null, conf, null);
    webServer.start();

    startTime = System.currentTimeMillis();
    serverRunner = new ThriftServerRunner(conf);
    if (blocking) {
      // run in Blocking mode
      serverRunner.run();
    } else {
      serverRunner.start();
    }
  }

  public void stop() {
    try {
      if (webServer != null) {
        webServer.stop();
      }
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }

    try {
      if (serverRunner != null) {
        serverRunner.stopServer();
      }
    } catch (Exception e) {
      LOG.warn(e.getMessage(), e);
    }
  }

  public void dumpThread(Writer writer) {
    PrintWriter stream = new PrintWriter(writer);
    int STACK_DEPTH = 20;
    boolean contention = threadBean.isThreadContentionMonitoringEnabled();
    long[] threadIds = threadBean.getAllThreadIds();
    stream.println("Process Thread Dump: Tajo Worker");
    stream.println(threadIds.length + " active threads");
    for (long tid : threadIds) {
      ThreadInfo info = threadBean.getThreadInfo(tid, STACK_DEPTH);
      if (info == null) {
        stream.println("  Inactive");
        continue;
      }
      stream.println("Thread " + getThreadTaskName(info.getThreadId(), info.getThreadName()) + ":");
      Thread.State state = info.getThreadState();
      stream.println("  State: " + state + ", Blocked count: " + info.getBlockedCount() +
          ", Waited count: " + info.getWaitedCount());
      if (contention) {
        stream.println("  Blocked time: " + info.getBlockedTime() + ", Waited time: " + info.getWaitedTime());
      }
      if (state == Thread.State.WAITING) {
        stream.println("  Waiting on " + info.getLockName());
      } else if (state == Thread.State.BLOCKED) {
        stream.println("  Blocked on " + info.getLockName() +
            ", Blocked by " + getThreadTaskName(info.getLockOwnerId(), info.getLockOwnerName()));
      }
      stream.println("  Stack:");
      for (StackTraceElement frame : info.getStackTrace()) {
        stream.println("    " + frame.toString());
      }
      stream.println("");
    }
  }

  String getThreadTaskName(long id, String name) {
    if (name == null) {
      return Long.toString(id);
    }
    return id + " (" + name + ")";
  }

  public class ThriftServerContext {
    public String getServerName() {
      return serverRunner.getAddress();
    }

    public long getStartTime() {
      return startTime;
    }

    public Collection<QueryProgressInfo> getQuerySubmitTasks() {
      return serverRunner.getTajoThriftServiceImpl().getQuerySubmitTasks();
    }

    public Collection<TajoThriftServiceImpl.TajoClientHolder> getTajoClientSessions() {
      return serverRunner.getTajoThriftServiceImpl().getTajoClientSessions();
    }

    public TajoThriftService.Iface getTajoThriftService() {
      return serverRunner.getTajoThriftServiceImpl();
    }

    public TajoConf getConfig() {
      return conf;
    }
  }

  public static void main(String[] args) throws Exception {
    (new TajoThriftServer(new TajoConf())).startServer(args, true);
  }
}
