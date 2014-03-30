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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.DDLBuilder;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
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
    formatter.printHelp( "tajo_dump [options] [database name]", options);
  }

  private static Pair<String, Integer> getConnectionAddr(TajoConf conf, CommandLine cmd) {
    String hostName = null;
    Integer port = null;
    if (cmd.hasOption("h")) {
      hostName = cmd.getOptionValue("h");
    }
    if (cmd.hasOption("p")) {
      port = Integer.parseInt(cmd.getOptionValue("p"));
    }

    if(hostName == null) {
      if (conf.getVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS) != null) {
        hostName = conf.getVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS).split(":")[0];
      }
    }
    if (port == null) {
      if (conf.getVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS) != null) {
        port = Integer.parseInt(conf.getVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS).split(":")[1]);
      }
    }
    return new Pair<String, Integer>(hostName, port);
  }

  public static void main(String [] args) throws ParseException, IOException, ServiceException, SQLException {
    final TajoConf conf = new TajoConf();
    final CommandLineParser parser = new PosixParser();
    final CommandLine cmd = parser.parse(options, args);
    final Pair<String, Integer> hostAndPort = getConnectionAddr(conf, cmd);
    final String hostName = hostAndPort.getFirst();
    final Integer port = hostAndPort.getSecond();
    final UserGroupInformation userInfo = UserGroupInformation.getCurrentUser();

    String baseDatabaseName = null;
    if (cmd.getArgList().size() > 0) {
      baseDatabaseName = (String) cmd.getArgList().get(0);
    }

    boolean isDumpingAllDatabases = cmd.hasOption('a');

    // Neither two choices
    if (!isDumpingAllDatabases && baseDatabaseName == null) {
      printUsage();
      System.exit(-1);
    }

    TajoClient client = null;
    if ((hostName == null) ^ (port == null)) {
      System.err.println("ERROR: cannot find any TajoMaster rpc address in arguments and tajo-site.xml.");
      System.exit(-1);
    } else if (hostName != null && port != null) {
      conf.setVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS, hostName+":"+port);
      client = new TajoClient(conf);
    } else if (hostName == null && port == null) {
      client = new TajoClient(conf);
    }

    PrintWriter writer = new PrintWriter(System.out);

    printHeader(writer, userInfo);

    if (isDumpingAllDatabases) {
      for (String databaseName : client.getAllDatabaseNames()) {
        dumpDatabase(client, databaseName, writer);
      }
    } else {
      dumpDatabase(client, baseDatabaseName, writer);
    }
    client.close();

    writer.flush();
    writer.close();
    System.exit(0);
  }

  private static void printHeader(PrintWriter writer, UserGroupInformation userInfo) {
    writer.write("--\n");
    writer.write("-- Tajo database dump\n");
    writer.write("--\n");
    writer.write("-- Dump user: " + userInfo.getUserName() + "\n");
    writer.write("-- Dump date: " + toDateString() + "\n");
    writer.write("--\n");
    writer.write("\n");
  }

  private static void dumpDatabase(TajoClient client, String databaseName, PrintWriter writer)
      throws SQLException, ServiceException {
    writer.write("\n");
    writer.write("--\n");
    writer.write(String.format("-- Database name: %s%n", databaseName));
    writer.write("--\n");
    writer.write("\n");
    writer.write(String.format("CREATE DATABASE IF NOT EXISTS %s;", databaseName));
    writer.write("\n");

    // returned list is immutable.
    List<String> tableNames = TUtil.newList(client.getTableList(databaseName));
    Collections.sort(tableNames);
    for (String tableName : tableNames) {
      try {
        TableDesc table =
            client.getTableDesc(CatalogUtil.denormalizeIdentifier(CatalogUtil.buildFQName(databaseName,tableName)));
        if (table.isExternal()) {
          writer.write(DDLBuilder.buildDDLForExternalTable(table));
        } else {
          writer.write(DDLBuilder.buildDDLForBaseTable(table));
        }
        writer.write("\n\n");
      } catch (Exception e) {
        // dump for each table can throw any exception. We need to skip the exception case.
        // here, the error message prints out via stderr.
        System.err.println("ERROR:" + tableName + "," + e.getMessage());
      }
    }
  }

  private static String toDateString() {
    DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
    java.util.Date today = Calendar.getInstance().getTime();
    return df.format(today);
  }
}
