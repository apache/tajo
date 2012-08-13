package nta.common;

/**
 * @author jihoon
 */
public class Sleeper {
  private long before;
  private long cur;

  public Sleeper() {
    before = -1;
  }

  public void sleep(long time) throws InterruptedException {
    long sleeptime;
    cur = System.currentTimeMillis();
    if (before == -1) {
      sleeptime = time;
    } else {
      sleeptime = time - (cur - before);
    }
    if (sleeptime > 0) {
      Thread.sleep(sleeptime);
    }
    before = System.currentTimeMillis();
  }
}