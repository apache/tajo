package nta.rpc;

public interface Callback<T> {
  public void onComplete(T response);

  public void onFailure(Throwable error);
}
