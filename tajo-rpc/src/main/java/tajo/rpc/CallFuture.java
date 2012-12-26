/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.rpc;

import java.util.concurrent.*;

@Deprecated
class CallFuture implements Future<Object> {
  private Semaphore sem = new Semaphore(0);
  private Object response = null;
  @SuppressWarnings("rawtypes")
  private Class returnType;

  @SuppressWarnings("rawtypes")
  public CallFuture(Class returnType) {
    this.returnType = returnType;
  }

  @SuppressWarnings("rawtypes")
  public Class getReturnType() {
    return this.returnType;
  }

  @Override
  public boolean cancel(boolean arg0) {
    return false;
  }

  @Override
  public Object get() throws InterruptedException, ExecutionException {
    sem.acquire();
    return response;
  }

  @Override
  public Object get(long timeout, TimeUnit unit) throws InterruptedException,
      ExecutionException, TimeoutException {
    if (sem.tryAcquire(timeout, unit)) {
      return response;
    } else {
      throw new TimeoutException();
    }
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return sem.availablePermits() > 0;
  }

  public void setResponse(Object response) {
    this.response = response;
    sem.release();
  }
}
