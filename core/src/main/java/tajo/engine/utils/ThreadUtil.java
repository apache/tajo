package tajo.engine.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.PrintWriter;
import java.lang.Thread.UncaughtExceptionHandler;

public class ThreadUtil {
	protected static final Log LOG = LogFactory.getLog(ThreadUtil.class);

	  /**
	   * Utility method that sets name, daemon status and starts passed thread.
	   * @param t thread to run
	   * @return Returns the passed Thread <code>t</code>.
	   */
	  public static Thread setDaemonThreadRunning(final Thread t) {
	    return setDaemonThreadRunning(t, t.getName());
	  }

	  /**
	   * Utility method that sets name, daemon status and starts passed thread.
	   * @param t thread to frob
	   * @param name new name
	   * @return Returns the passed Thread <code>t</code>.
	   */
	  public static Thread setDaemonThreadRunning(final Thread t,
	    final String name) {
	    return setDaemonThreadRunning(t, name, null);
	  }

	  /**
	   * Utility method that sets name, daemon status and starts passed thread.
	   * @param t thread to frob
	   * @param name new name
	   * @param handler A handler to set on the thread.  Pass null if want to
	   * use default handler.
	   * @return Returns the passed Thread <code>t</code>.
	   */
	  public static Thread setDaemonThreadRunning(final Thread t,
	    final String name, final UncaughtExceptionHandler handler) {
	    t.setName(name);
	    if (handler != null) {
	      t.setUncaughtExceptionHandler(handler);
	    }
	    t.setDaemon(true);
	    t.start();
	    return t;
	  }

	  /**
	   * Shutdown passed thread using isAlive and join.
	   * @param t Thread to shutdown
	   */
	  public static void shutdown(final Thread t) {
	    shutdown(t, 0);
	  }

	  /**
	   * Shutdown passed thread using isAlive and join.
	   * @param joinwait Pass 0 if we're to wait forever.
	   * @param t Thread to shutdown
	   */
	  public static void shutdown(final Thread t, final long joinwait) {
	    if (t == null) return;
	    while (t.isAlive()) {
	      try {
	        t.join(joinwait);
	      } catch (InterruptedException e) {
	        LOG.warn(t.getName() + "; joinwait=" + joinwait, e);
	      }
	    }
	  }


	  /**
	   * @param t Waits on the passed thread to die dumping a threaddump every
	   * minute while its up.
	   * @throws InterruptedException
	   */
	  public static void threadDumpingIsAlive(final Thread t)
	  throws InterruptedException {
	    if (t == null) {
	      return;
	    }

	    while (t.isAlive()) {
	      t.join(60 * 1000);
	      if (t.isAlive()) {
	        ReflectionUtils.printThreadInfo(new PrintWriter(System.out),
	            "Automatic Stack Trace every 60 seconds waiting on " +
	            t.getName());
	      }
	    }
	  }

	  /**
	   * @param millis How long to sleep for in milliseconds.
	   */
	  public static void sleep(int millis) {
	    try {
	      Thread.sleep(millis);
	    } catch (InterruptedException e) {
	      e.printStackTrace();
	    }
	  }

	  /**
	   * Sleeps for the given amount of time even if interrupted. Preserves
	   * the interrupt status.
	   * @param msToWait the amount of time to sleep in milliseconds
	   */
	  public static void sleepWithoutInterrupt(final long msToWait) {
	    long timeMillis = System.currentTimeMillis();
	    long endTime = timeMillis + msToWait;
	    boolean interrupted = false;
	    while (timeMillis < endTime) {
	      try {
	        Thread.sleep(endTime - timeMillis);
	      } catch (InterruptedException ex) {
	        interrupted = true;
	      }
	      timeMillis = System.currentTimeMillis();
	    }

	    if (interrupted) {
	      Thread.currentThread().interrupt();
	    }
	  }
}
