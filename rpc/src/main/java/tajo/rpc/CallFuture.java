package tajo.rpc;

import java.util.concurrent.*;

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
