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

package org.apache.tajo.client;

import com.google.protobuf.ServiceException;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ClientProtos.BriefQueryInfo;
import org.apache.tajo.ipc.ClientProtos.WorkerResourceInfo;
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

public class TajoGetConf {
  private static final org.apache.commons.cli.Options options;

  static {
    options = new Options();
    options.addOption("h", "host", true, "Tajo server host");
    options.addOption("p", "port", true, "Tajo server port");
    options.addOption("masters", null, false, "gets list of tajomasters in the cluster");
    options.addOption("confKey", null, true, "gets a specific key from the configuration");
  }

  private TajoConf tajoConf;
  private TajoClient tajoClient;
  private Writer writer;

  public TajoGetConf(TajoConf tajoConf, Writer writer) {
    this(tajoConf, writer, null);
  }

  public TajoGetConf(TajoConf tajoConf, Writer writer, TajoClient tajoClient) {
    this.tajoConf = tajoConf;
    this.writer = writer;
    this.tajoClient = tajoClient;
  }

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "getconf [options]", options );
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

    if (cmd.hasOption("masters")) {
      cmdType = 1;
    } else if (cmd.hasOption("confKey")) {
      cmdType = 2;
      param = cmd.getOptionValue("confKey");
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
      return;
    } else if (hostName != null && port != null) {
      tajoConf.setVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS, hostName + ":" + port);
      tajoClient = new TajoClient(tajoConf);
    } else if (hostName == null && port == null) {
      tajoClient = new TajoClient(tajoConf);
    }

    switch (cmdType) {
      case 1:
        processMasters(writer);
        break;
      case 2:
        processConfKey(writer, param);
        break;
      default:
        printUsage();
        break;
    }

    writer.flush();
  }

  private void processMasters(Writer writer) throws ParseException, IOException,
      ServiceException, SQLException {
    String confMasterServiceAddr = tajoConf.getVar(TajoConf.ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS);
    InetSocketAddress masterAddress = NetUtils.createSocketAddr(confMasterServiceAddr);
    writer.write(masterAddress.getHostName());
    writer.write("\n");
  }

  private void processConfKey(Writer writer, String param) throws ParseException, IOException,
      ServiceException, SQLException {
    String value = tajoConf.get(param);

    if (value != null) {
      writer.write(value);
    } else {
      writer.write("Configuration " + param + " is missing.");
    }

    writer.write("\n");
  }

  public static void main(String [] args) throws Exception {
    TajoConf conf = new TajoConf();

    Writer writer = new PrintWriter(System.out);
    try {
      TajoGetConf admin = new TajoGetConf(conf, writer);
      admin.runCommand(args);
    } finally {
      writer.close();
      System.exit(0);
    }
  }
}
