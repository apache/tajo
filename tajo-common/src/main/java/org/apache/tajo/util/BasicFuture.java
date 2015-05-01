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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BasicFuture<T> implements Future<T> {

  private T result;
  private Exception exception;
  private boolean finished;

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public synchronized boolean isDone() {
    return finished;
  }

  @Override
  public synchronized T get() throws InterruptedException, ExecutionException {
    while (!finished) {
      wait();
    }
    if (exception != null) {
      throw new ExecutionException(exception);
    }
    return result;
  }

  @Override
  public synchronized T get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    long remain = unit.toMillis(timeout);
    long prev = System.currentTimeMillis();
    while (!finished && remain > 0) {
      wait(remain);
      long current = System.currentTimeMillis();
      remain -= current - prev;
      prev = current;
    }
    if (!finished) {
      throw new TimeoutException("Timed-out");
    }
    if (exception != null) {
      throw new ExecutionException(exception);
    }
    return result;
  }

  public synchronized boolean done(T result) {
    try {
      if (finished) {
        return false;
      }
      this.result = result;
      this.finished = true;
      return true;
    } finally {
      notifyAll();
    }
  }

  public synchronized boolean failed(Exception ex) {
    try {
      if (finished) {
        return false;
      }
      this.exception = ex;
      this.finished = true;
      return true;
    } finally {
      notifyAll();
    }
  }
}

