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

package org.apache.tajo.rpc;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class NettyRpcController implements RpcController {
  private String errorText;

  @Override
  public void reset() {
    errorText = null;
  }

  @Override
  public boolean failed() {
    return errorText != null;
  }

  @Override
  public String errorText() {
    return errorText;
  }

  @Override
  public void startCancel() {
    // TODO - to be implemented
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFailed(String s) {
    errorText = s;
  }

  @Override
  public boolean isCanceled() {
    // TODO - to be implemented
    return false;
  }

  @Override
  public void notifyOnCancel(RpcCallback<Object> objectRpcCallback) {
    throw new UnsupportedOperationException();
  }
}
