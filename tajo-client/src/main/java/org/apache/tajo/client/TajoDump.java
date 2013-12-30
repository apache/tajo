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

import com.google.common.collect.Lists;
import com.google.protobuf.ServiceException;
import org.apache.commons.cli.*;
import org.apache.tajo.catalog.DDLBuilder;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

public class TajoDump {
  private static final org.apache.commons.cli.Options options;

  static {
    options = new Options();
    options.addOption("h", "host", true, "Tajo server host");
    options.addOption("p", "port", true, "Tajo server port");
    options.addOption("a", "all", false, "dump all table DDLs");
  }

  private static void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "tajo_dump [options] [table_name]", options );
  }

  public static void main(String [] args) throws ParseException, IOException, ServiceException, SQLException {
    TajoConf conf = new TajoConf();

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    String hostName = null;
    Integer port = null;
    if (cmd.hasOption("h")) {
      hostName = cmd.getOptionValue("h");
    }
    if (cmd.hasOption("p")) {
      port = Integer.parseInt(cmd.getOptionValue("p"));
    }

    // if there is no "-h" option,
    if(hostName == null) {
      if (conf.getVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS) != null) {
        // it checks if the client service address is given in configuration and distributed mode.
        // if so, it sets entryAddr.
        hostName = conf.getVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS).split(":")[0];
      }
    }
    if (port == null) {
      if (conf.getVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS) != null) {
        // it checks if the client service address is given in configuration and distributed mode.
        // if so, it sets entryAddr.
        port = Integer.parseInt(conf.getVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS).split(":")[1]);
      }
    }

    TajoClient client = null;
    if ((hostName == null) ^ (port == null)) {
      System.err.println("ERROR: cannot find valid Tajo server address");
      System.exit(-1);
    } else if (hostName != null && port != null) {
      conf.setVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS, hostName+":"+port);
      client = new TajoClient(conf);
    } else if (hostName == null && port == null) {
      client = new TajoClient(conf);
    }

    List<TableDesc> tableDescList = Lists.newArrayList();

    if (cmd.hasOption("a")) {
      for (String tableName : client.getTableList()) {
        tableDescList.add(client.getTableDesc(tableName));
      }
    } else if (cmd.getArgs().length > 0) {
      for (String tableName : cmd.getArgs()) {
        tableDescList.add(client.getTableDesc(tableName));
      }
    } else {
      printUsage();
    }


    Writer writer = new PrintWriter(System.out);
    writer.write("--\n");
    writer.write("-- Tajo database dump\n");
    writer.write("-- Dump date: " + toDateString() + "\n");
    writer.write("--\n");
    writer.write("\n");
    for (TableDesc tableDesc : tableDescList) {
      writer.write(DDLBuilder.buildDDL(tableDesc));
      writer.write("\n\n");
    }
    writer.flush();
    writer.close();
    System.exit(0);
  }

  private static String toDateString() {
    DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    java.util.Date today = Calendar.getInstance().getTime();
    return df.format(today);
  }
}
