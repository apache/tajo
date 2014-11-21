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

package org.apache.tajo.thrift.cli;

import org.apache.tajo.thrift.cli.TajoThriftCli.TajoThriftCliContext;
import org.apache.tajo.thrift.generated.TGetQueryStatusResponse;
import org.apache.tajo.thrift.generated.TTableDesc;

import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.ResultSet;

public interface TajoThriftCliOutputFormatter {
  /**
   * Initialize formatter
   * @param context
   */
  public void init(TajoThriftCliContext context);

  /**
   * print query result to console
   * @param sout
   * @param sin
   * @param tableDesc
   * @param responseTime
   * @param res
   * @throws Exception
   */
  public void printResult(PrintWriter sout, InputStream sin, TTableDesc tableDesc,
                          float responseTime, ResultSet res) throws Exception;

  /**
   * print no result message
   * @param sout
   */
  public void printNoResult(PrintWriter sout);

  /**
   * print simple message
   * @param sout
   * @param message
   */
  public void printMessage(PrintWriter sout, String message);

  /**
   * print query progress message
   * @param sout
   * @param status
   */
  public void printProgress(PrintWriter sout, TGetQueryStatusResponse status);

  /**
   * print error message
   * @param sout
   * @param t
   */
  public void printErrorMessage(PrintWriter sout, Throwable t);

  /**
   * print error message
   * @param sout
   * @param message
   */
  public void printErrorMessage(PrintWriter sout, String message);

  /**
   * print error message
   * @param sout
   * @param queryId
   */
  public void printKilledMessage(PrintWriter sout, String queryId);

  /**
   * print query status error message
   * @param sout
   * @param status
   */
  void printErrorMessage(PrintWriter sout, TGetQueryStatusResponse status);

  void setScirptMode();
}
