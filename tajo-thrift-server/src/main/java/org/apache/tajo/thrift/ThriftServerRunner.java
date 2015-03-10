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

package org.apache.tajo.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.thrift.generated.TajoThriftService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class ThriftServerRunner extends Thread implements ThriftServerConstants {
  private static final Log LOG = LogFactory.getLog(ThriftServerRunner.class);

  private TajoConf conf;
  private TServer tserver;
  private TajoThriftServiceImpl tajoThriftService;
  private String address;

  public ThriftServerRunner(TajoConf conf) {
    this.conf = conf;
  }

  public TajoThriftServiceImpl getTajoThriftServiceImpl() {
    return tajoThriftService;
  }
  /**
   * Setting up the thrift TServer
   */
  public void setupServer() throws Exception {
    this.tajoThriftService = new TajoThriftServiceImpl(conf);

    TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
    TTransportFactory transportFactory =new TTransportFactory();

    TajoThriftService.Processor<TajoThriftService.Iface> processor =
        new TajoThriftService.Processor<TajoThriftService.Iface>(tajoThriftService);

    int listenPort = conf.getInt(SERVER_PORT_CONF_KEY, DEFAULT_LISTEN_PORT);

    InetAddress listenAddress = getBindAddress();
    TServerSocket serverTransport = new TServerSocket(new InetSocketAddress(listenAddress, listenPort));
    TBoundedThreadPoolServer.Args serverArgs = new TBoundedThreadPoolServer.Args(serverTransport, conf);
    serverArgs.processor(processor)
        .transportFactory(transportFactory)
        .protocolFactory(protocolFactory);

    TBoundedThreadPoolServer tserver = new TBoundedThreadPoolServer(serverArgs);
    this.tserver = tserver;

    if (listenAddress.isAnyLocalAddress()) {
      this.address = InetAddress.getLocalHost().getHostName() + ":" + serverTransport.getServerSocket().getLocalPort();
    } else {
      this.address = serverTransport.getServerSocket().getInetAddress().getHostName() + ":" +
          serverTransport.getServerSocket().getLocalPort();
    }
  }

  public String getAddress() {
    return address;
  }

  private InetAddress getBindAddress() throws UnknownHostException {
    String bindAddressStr = conf.get(SERVER_ADDRESS_CONF_KEY, DEFAULT_BIND_ADDRESS);
    return InetAddress.getByName(bindAddressStr);
  }

  public void stopServer() {
    if (tajoThriftService != null) {
      tajoThriftService.stop();
    }

    if (tserver != null) {
      tserver.stop();
    }
    LOG.info("Tajo Thrift Server stopped: " + this.address);
  }

  /*
 * Runs the Thrift server
 */
  @Override
  public void run() {
    try {
      setupServer();

      LOG.info("starting Tajo Thrift server on " + address);

      tserver.serve();
    } catch (Exception e) {
      LOG.fatal("Cannot run ThriftServer", e);
      // Crash the process if the ThriftServer is not running
      System.exit(-1);
    }
  }
}
