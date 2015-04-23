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

package org.apache.tajo.rpc.test.impl;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.rpc.test.DummyProtocol.DummyProtocolService.Interface;
import org.apache.tajo.rpc.test.TestProtos.EchoMessage;
import org.apache.tajo.rpc.test.TestProtos.SumRequest;
import org.apache.tajo.rpc.test.TestProtos.SumResponse;

public class DummyProtocolAsyncImpl implements Interface {
  private static final Log LOG =
      LogFactory.getLog(DummyProtocolAsyncImpl.class);
  public boolean getNullCalled = false;
  public boolean getErrorCalled = false;

  @Override
  public void sum(RpcController controller, SumRequest request,
                  RpcCallback<SumResponse> done) {

    SumResponse response = SumResponse.newBuilder().setResult(
        request.getX1()+request.getX2()+request.getX3()+request.getX4()
    ).build();
    done.run(response);
  }

  @Override
  public void echo(RpcController controller, EchoMessage request,
                   RpcCallback<EchoMessage> done) {

    done.run(request);
  }

  @Override
  public void getError(RpcController controller, EchoMessage request,
                       RpcCallback<EchoMessage> done) {
    LOG.info("noCallback is called");
    getErrorCalled = true;
    controller.setFailed(request.getMessage());
    done.run(request);
  }

  @Override
  public void getNull(RpcController controller, EchoMessage request,
                      RpcCallback<EchoMessage> done) {
    getNullCalled = true;
    LOG.info("noCallback is called");
    done.run(null);
  }

  @Override
  public void delay(RpcController controller, EchoMessage request,
                    RpcCallback<EchoMessage> done) {
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
    }

    done.run(request);
  }

  @Override
  public void busy(RpcController controller, EchoMessage request,
                   RpcCallback<EchoMessage> done) {
    try {
      Thread.sleep(30000);
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
    }
    done.run(request);
  }

  @Override
  public void throwException(RpcController controller, EchoMessage request,
                             RpcCallback<EchoMessage> done) {
    if(controller != null) {
      controller.setFailed("throwException");
      done.run(request);
    } else {
      throw new RuntimeException("throwException");
    }
  }
}