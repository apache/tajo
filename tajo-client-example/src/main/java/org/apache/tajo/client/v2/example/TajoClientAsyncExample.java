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
import org.apache.tajo.client.v2.QueryFuture;
import org.apache.tajo.client.v2.TajoClient;
import org.apache.tajo.client.v2.exception.ClientUnableToConnectException;
import org.apache.tajo.exception.TajoException;

import java.util.concurrent.ExecutionException;

public class TajoClientAsyncExample {

  public static void run(String hostname, int port, String sql) throws ClientUnableToConnectException {

    try (TajoClient client = new TajoClient(hostname, port)) {

      try (QueryFuture future = client.executeQueryAsync(sql)) {

        while (!future.isDone()) { // isDone will be true if query state becomes success, failed, or killed.
          System.out.println("progress: " + future.progress());
        }

        if (future.isSuccessful()) {
          System.out.println(ResultSetUtil.prettyFormat(future.get()));
        }

      } catch (TajoException e) {
        // executeQueryAsync() directly throws a TajoException instance if a query syntax is wrong.
        e.printStackTrace();
      } catch (ExecutionException e) {
        // e.getCause() contains an TajoException caused by a running query.
        System.err.println(e.getCause().getMessage());
      } catch (Throwable t) {
        System.err.println(t.getMessage());
      }
    }
  }

  public static void main(String[] args) throws ClientUnableToConnectException {
    if (args.length < 3) {
      System.err.println(String.format("usage: java -cp [classpath] %s [hostname] [port] sql",
          TajoClientAsyncExample.class.getCanonicalName()));
      System.exit(-1);
    }

    StringBuilder sqlBuilder = new StringBuilder();
    for (int i = 2; i < args.length; i++) {
      sqlBuilder.append(args[i]).append(" ");
    }

    run(args[0], Integer.parseInt(args[1]), sqlBuilder.toString());
  }
}
