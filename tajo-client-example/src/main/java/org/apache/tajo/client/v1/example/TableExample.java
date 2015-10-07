/*
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

package org.apache.tajo.client.v1.example;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.exception.DuplicateDatabaseException;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.exception.UndefinedTableException;
import org.apache.tajo.util.KeyValueSet;

import java.net.InetSocketAddress;
import java.sql.SQLException;

public class TableExample {
  public static void run(String hostname, int port) throws SQLException, TajoException{
    TajoClient client = new TajoClientImpl(new InetSocketAddress(hostname, port), "default", new KeyValueSet());

    try {
      client.createDatabase("example");
    } catch (DuplicateDatabaseException e) {
      throw new RuntimeException("database 'example' already exists");
    }

    if (!client.getAllDatabaseNames().contains("example")) {
      throw new RuntimeException("Database creation was failed");
    }

    // select base database
    try {
      client.selectDatabase("example");
    } catch (UndefinedDatabaseException e) {
      throw new RuntimeException("No such a database");
    }

    // It will create a table 'table' in 'example' database.
    client.updateQuery("CREATE TABLE employee (name TEXT, age int, dept TEXT)");

    TableDesc tableDesc;
    try {
      tableDesc = client.getTableDesc("example.employee");
    } catch (UndefinedTableException t) {
      throw new RuntimeException("No such a table");
    }

    System.out.println("Table name: " + tableDesc.getName());
    System.out.println("Table uri:  " + tableDesc.getUri().toASCIIString());
    System.out.println("Table schema: ");

    // for each column, print its name and data type
    for (Column c: tableDesc.getSchema().getAllColumns()) {
      System.out.println(String.format("  name: %s, type: %s", c.getSimpleName(), c.getDataType().getType().name()));
    }

    client.close();
  }

  public static void main(String[] args) throws TajoException, SQLException {
    if (args.length < 2) {
      System.err.println(String.format("usage: java -cp [classpath] %s [hostname] [port]",
          TableExample.class.getCanonicalName()));
      System.exit(-1);
    }

    run(args[0], Integer.parseInt(args[1]));
  }
}
