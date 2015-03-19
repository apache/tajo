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

import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CallFuture<T> implements RpcCallback<T>, Future<T> {

  private final Semaphore sem = new Semaphore(0);
  private boolean done = false;
  private T response;
  private RpcController controller;

  public CallFuture() {
    controller = new DefaultRpcController();
  }

  public RpcController getController() {
    return controller;
  }

  @Override
  public void run(T t) {
    this.response = t;
    done = true;
    sem.release();
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    controller.startCancel();
    sem.release();
    return controller.isCanceled();
  }

  @Override
  public boolean isCancelled() {
    return controller.isCanceled();
  }

  @Override
  public boolean isDone() {
    return done;
  }

  @Override
  public T get() throws InterruptedException {
    sem.acquire();

    return response;
  }

  @Override
  public T get(long timeout, TimeUnit unit)
      throws InterruptedException, TimeoutException {
    if (sem.tryAcquire(timeout, unit)) {
      return response;
    } else {
      throw new TimeoutException();
    }
  }
}
