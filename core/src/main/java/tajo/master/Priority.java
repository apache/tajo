package tajo.master;

/**
 * @author jihoon
 */
public class Priority implements Comparable<Priority> {
  private int priority;

  public Priority(int prio) {
    set(prio);
  }

  public void set(int prio) {
    this.priority = prio;
  }

  public int get() {
    return this.priority;
  }

  @Override
  public int compareTo(Priority o) {
    return this.get() - o.get();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Priority) {
      Priority p = (Priority) o;
      return p.get() == this.get();
    }
    return false;
  }

  @Override
  public String toString() {
    return "" + priority;
  }
}
