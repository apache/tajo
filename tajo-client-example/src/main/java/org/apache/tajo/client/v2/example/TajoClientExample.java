/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.client.v2.example;

import org.apache.tajo.client.ResultSetUtil;
import org.apache.tajo.client.v2.TajoClient;
import org.apache.tajo.client.v2.exception.ClientUnableToConnectException;
import org.apache.tajo.exception.QueryFailedException;
import org.apache.tajo.exception.QueryKilledException;
import org.apache.tajo.exception.TajoException;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TajoClientExample {

  public static void run(String hostname, int port, String sql) throws ClientUnableToConnectException {

    try (TajoClient client = new TajoClient(hostname, port)) {

      try {
        ResultSet result = client.executeQuery(sql);
        System.out.println(ResultSetUtil.prettyFormat(result));

      } catch (QueryFailedException e) {
        System.err.println("query is failed.");
      } catch (QueryKilledException e) {
        System.err.println("query is killed.");
      } catch (SQLException | TajoException e) {
        e.printStackTrace();
      }

    }
  }

  public static void main(String[] args) throws ClientUnableToConnectException {
    if (args.length < 3) {
      System.err.println(String.format("usage: java -cp [classpath] %s [hostname] [port] sql",
          TajoClientExample.class.getCanonicalName()));
      System.exit(-1);
    }

    StringBuilder sqlBuilder = new StringBuilder();
    for (int i = 2; i < args.length; i++) {
      sqlBuilder.append(args[i]).append(" ");
    }

    run(args[0], Integer.parseInt(args[1]), sqlBuilder.toString());
  }
}
