package nta.rpc;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Callback<T> implements Future<T> {
  public enum Status {
    READY, SUCCESS, FAILURE
  }

  private Status status;
  private Semaphore sem = new Semaphore(0);
  private T result = null;
  private Throwable err;

  public Callback() {
    status = Status.READY;
  }

  public void onComplete(T response) {
    status = Status.SUCCESS;
    result = response;
    sem.release();
  }

  public void onFailure(Throwable error) {
    status = Status.FAILURE;
    result = null;
    err = error;
    sem.release();
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    if (!didGetResponse()) {
      sem.acquire();
    }
    return result;
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException,
      ExecutionException, TimeoutException {
    if (!didGetResponse()) {
      if (sem.tryAcquire(timeout, unit)) {
        return result;
      } else {
        throw new TimeoutException();
      }
    }
    return result;
  }

  public boolean didGetResponse() {
    return (isSuccess() || isFailure());
  }

  public boolean isSuccess() {
    return (status == Status.SUCCESS);
  }

  public boolean isFailure() {
    return (status == Status.FAILURE);
  }

  public String getErrorMessage() {
    if (status == Status.SUCCESS) {
      return "";
    }
    return err.getMessage();
  }

  @Override
  public boolean cancel(boolean arg0) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return sem.availablePermits() > 0;
  }
}
