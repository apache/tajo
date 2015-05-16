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
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.service.ServiceTrackerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;

public class TajoHAAdmin {
  private static final Options options;

  static {
    options = new Options();
    options.addOption("h", "host", true, "Tajo server host");
    options.addOption("p", "port", true, "Tajo server port");
    options.addOption("transitionToActive", null, true, "Transitions the master into Active state");
    options.addOption("transitionToBackup", null, true, "Transitions the master into Backup state");
    options.addOption("getState", null, true, "Returns the state of the master");
    options.addOption("formatHA", null, false, "Format HA status on share storage");
  }

  private TajoConf tajoConf;
  private Writer writer;
  private ServiceTracker serviceTracker;

  public TajoHAAdmin(TajoConf tajoConf, Writer writer) {
    this(tajoConf, writer, null);
  }

  public TajoHAAdmin(TajoConf tajoConf, Writer writer, TajoClient tajoClient) {
    this.tajoConf = tajoConf;
    this.writer = writer;
    serviceTracker = ServiceTrackerFactory.get(this.tajoConf);
  }

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "haadmin [options]", options );
  }

  public void runCommand(String[] args) throws Exception {
    if(args.length == 1 &&
        (args[0].equalsIgnoreCase("-transitionToActive")
            || args[0].equalsIgnoreCase("-transitionToBackup")
            || args[0].equalsIgnoreCase("-getState"))) {
      writer.write("Not enough arguments: expected 1 but got 0\n");
      writer.flush();
      return;
    }

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

    if (cmd.hasOption("transitionToActive")) {
      cmdType = 1;
      param = cmd.getOptionValue("transitionToActive");
    } else if (cmd.hasOption("transitionToBackup")) {
      cmdType = 2;
      param = cmd.getOptionValue("transitionToBackup");
    } else if (cmd.hasOption("getState")) {
      cmdType = 3;
      param = cmd.getOptionValue("getState");
    } else if (cmd.hasOption("formatHA")) {
      cmdType = 4;
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
    }

    if (!tajoConf.getBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE)) {
      writer.write("HA is not enabled for this tajo cluster.");
    } else {
      switch (cmdType) {
        case 1:
          writer.write("Not Yet Implemented\n");
          break;
        case 2:
          writer.write("Not Yet Implemented\n");
          break;
        case 3:
          getState(writer, param);
          break;
        case 4:
          formatHA(writer);
          break;
        default:
          printUsage();
          break;
      }
    }

    writer.flush();
  }

  private void getState(Writer writer, String param) throws ParseException, IOException,
      ServiceException {

    int retValue = serviceTracker.getState(param, tajoConf);

    switch (retValue) {
      case 1:
        writer.write("The master is active.\n");
        break;
      case 0:
        writer.write("The master is backup.\n");
        break;
      case -1:
        writer.write("Finding failed. - master:" + param + "\n");
        break;
      default:
        writer.write("Cannot find the master. - master:" + param + "\n");
        break;
    }
  }

  private void formatHA(Writer writer) throws ParseException, IOException,
      ServiceException {
    int retValue = serviceTracker.formatHA(tajoConf);

    switch (retValue) {
      case 1:
        writer.write("Formatting finished successfully.\n");
        break;
      case 0:
        writer.write("If you want to format the ha information, you must shutdown tajo masters "
            + " before formatting.\n");
        break;
      default:
        writer.write("Cannot format ha information.\n");
        break;
    }
  }

  public static void main(String [] args) throws Exception {
    TajoConf conf = new TajoConf();

    Writer writer = new PrintWriter(System.out);
    try {
      TajoHAAdmin admin = new TajoHAAdmin(conf, writer);
      admin.runCommand(args);
    } finally {
      writer.close();
      System.exit(0);
    }
  }
}
