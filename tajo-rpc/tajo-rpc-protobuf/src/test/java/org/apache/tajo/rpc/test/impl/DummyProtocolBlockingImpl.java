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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.rpc.test.DummyProtocol.DummyProtocolService.BlockingInterface;
import org.apache.tajo.rpc.test.TestProtos.EchoMessage;
import org.apache.tajo.rpc.test.TestProtos.SumRequest;
import org.apache.tajo.rpc.test.TestProtos.SumResponse;

public class DummyProtocolBlockingImpl implements BlockingInterface {
  private static final Log LOG =
      LogFactory.getLog(DummyProtocolBlockingImpl.class);
  public boolean getNullCalled = false;
  public boolean getErrorCalled = false;

  @Override
  public SumResponse sum(RpcController controller, SumRequest request)
      throws ServiceException {
    return SumResponse.newBuilder().setResult(
        request.getX1()+request.getX2()+request.getX3()+request.getX4()
    ).build();
  }

  @Override
  public EchoMessage echo(RpcController controller, EchoMessage request)
      throws ServiceException {
    return EchoMessage.newBuilder().
        setMessage(request.getMessage()).build();
  }

  @Override
  public EchoMessage getError(RpcController controller, EchoMessage request)
      throws ServiceException {
    getErrorCalled = true;
    controller.setFailed(request.getMessage());
    return request;
  }

  @Override
  public EchoMessage getNull(RpcController controller, EchoMessage request)
      throws ServiceException {
    getNullCalled = true;
    LOG.info("noCallback is called");
    return null;
  }

  @Override
  public EchoMessage delay(RpcController controller, EchoMessage request)
      throws ServiceException {
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      //throw new ServiceException(e.getMessage(), e);
    }

    return request;
  }

  @Override
  public EchoMessage busy(RpcController controller, EchoMessage request) {
    try {
      Thread.sleep(30000);
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
    }
    return request;
  }

  @Override
  public EchoMessage throwException(RpcController controller, EchoMessage request)
      throws ServiceException {
    throw new ServiceException("Exception Test");
  }
}