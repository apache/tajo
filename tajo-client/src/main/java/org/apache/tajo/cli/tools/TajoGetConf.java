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
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.conf.TajoConf;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.SQLException;

public class TajoGetConf {
  private static final org.apache.commons.cli.Options options;

  static {
    options = new Options();
    options.addOption("h", "host", true, "Tajo server host");
    options.addOption("p", "port", true, "Tajo server port");
  }

  private TajoConf tajoConf;
  private TajoClient tajoClient;
  private Writer writer;

  public final static String defaultLeftPad = " ";
  public final static String defaultDescPad = "   ";

  public TajoGetConf(TajoConf tajoConf, Writer writer) {
    this(tajoConf, writer, null);
  }

  public TajoGetConf(TajoConf tajoConf, Writer writer, TajoClient tajoClient) {
    this.tajoConf = tajoConf;
    this.writer = writer;
    this.tajoClient = tajoClient;
  }

  private void printUsage(boolean tsqlMode) {
    if (!tsqlMode) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "getconf <key> [options]", options );
    }
    System.out.println(defaultLeftPad + "key" + defaultDescPad + "gets a specific key from the configuration");
  }

  public void runCommand(String[] args) throws Exception {
    runCommand(args, true);
  }

  public void runCommand(String[] args, boolean tsqlMode) throws Exception {
    CommandLineParser parser = new PosixParser();

    if (args.length == 0) {
      printUsage(tsqlMode);
      return;
    }

    CommandLine cmd = parser.parse(options, args);

    String hostName = null;
    Integer port = null;
    if (cmd.hasOption("h")) {
      hostName = cmd.getOptionValue("h");
    }
    if (cmd.hasOption("p")) {
      port = Integer.parseInt(cmd.getOptionValue("p"));
    }

    String param;
    if (cmd.getArgs().length > 1) {
      printUsage(tsqlMode);
      return;
    } else {
      param = cmd.getArgs()[0];
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

    if ((hostName == null) ^ (port == null)) {
      return;
    } else if (hostName != null && port != null) {
      tajoConf.setVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS, hostName + ":" + port);
      tajoClient = new TajoClientImpl(tajoConf);
    } else if (hostName == null && port == null) {
      tajoClient = new TajoClientImpl(tajoConf);
    }

    processConfKey(writer, param);
    writer.flush();
  }

  private void processConfKey(Writer writer, String param) throws ParseException, IOException,
      ServiceException, SQLException {
    String value = tajoConf.getTrimmed(param);

    // If there is no value in the configuration file, we need to find all ConfVars.
    if (value == null) {
      for(TajoConf.ConfVars vars : TajoConf.ConfVars.values()) {
        if (vars.varname.equalsIgnoreCase(param)) {
          value = tajoConf.getVar(vars);
          break;
        }
      }
    }

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
      admin.runCommand(args, false);
    } finally {
      writer.close();
      System.exit(0);
    }
  }
}
