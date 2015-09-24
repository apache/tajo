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

package org.apache.tajo.util;

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.rpc.RpcConstants;
import org.junit.Test;

import java.util.Properties;

import static org.apache.tajo.rpc.RpcConstants.CLIENT_CONNECTION_TIMEOUT;
import static org.apache.tajo.rpc.RpcConstants.CLIENT_RETRY_NUM;
import static org.junit.Assert.*;

public class TestRpcParamFactory {

  @Test
  public void testGetDefaults() throws Exception {
    TajoConf conf = new TajoConf();
    Properties defaultParams = RpcParameterFactory.get(conf);
    assertEquals(
        ConfVars.RPC_CLIENT_RETRY_NUM.defaultVal, defaultParams.getProperty(CLIENT_RETRY_NUM));
    assertEquals(
        ConfVars.RPC_CLIENT_CONNECTION_TIMEOUT.defaultVal, defaultParams.getProperty(CLIENT_CONNECTION_TIMEOUT));
    assertEquals(
        ConfVars.RPC_CLIENT_SOCKET_TIMEOUT.defaultVal, defaultParams.getProperty(RpcConstants.CLIENT_SOCKET_TIMEOUT));
  }

  @Test
  public void testGet() throws Exception {
    TajoConf conf = new TajoConf();
    conf.setIntVar(ConfVars.RPC_CLIENT_RETRY_NUM, 100);
    conf.setLongVar(ConfVars.RPC_CLIENT_CONNECTION_TIMEOUT, (long)(10 * 1000));
    conf.setLongVar(ConfVars.RPC_CLIENT_SOCKET_TIMEOUT, (long)60 * 1000);

    Properties defaultParams = RpcParameterFactory.get(conf);
    assertEquals("100", defaultParams.getProperty(CLIENT_RETRY_NUM));
    assertEquals(10 * 1000 + "", defaultParams.getProperty(CLIENT_CONNECTION_TIMEOUT));
    assertEquals(60 * 1000 + "", defaultParams.getProperty(RpcConstants.CLIENT_SOCKET_TIMEOUT));
  }
}