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

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

// message exchange between threads which can be only a success(run) or fail(cancel)
// successful cancel will make all following run() invocations to cancel the object, by calling cancel(T)
public class CancelableRpcCallback<T> implements RpcCallback<T> {

  private static final int INITIAL = 0;
  private static final int RESULT = 1;
  private static final int CANCELED = 2;

  private volatile T result;
  private final AtomicInteger state = new AtomicInteger(INITIAL);
  private final Semaphore semaphore = new Semaphore(0);

  @Override
  public void run(T result) {
    assert result != null;
    try {
      if (state.compareAndSet(INITIAL, RESULT)) {
        this.result = result;
      } else {
        cancel(result);
      }
    } finally {
      semaphore.release();
    }
  }

  public T cancel() {
    try {
      if (state.compareAndSet(INITIAL, CANCELED)) {
        return null;
      }
      return state.get() == RESULT ? result : null;
    } finally {
      semaphore.release();
    }
  }

  public T get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
    if (semaphore.tryAcquire(timeout, unit) && state.get() == RESULT) {
      return result;
    }
    throw new TimeoutException();
  }

  protected void cancel(T canceled) {
  }
}
