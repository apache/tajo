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

package org.apache.tajo.webapp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.SessionIdManager;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.*;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;
import org.mortbay.util.MultiException;

import javax.servlet.http.HttpServlet;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.BindException;
import java.net.URL;
import java.util.*;

/**
 * This class is borrowed from Hadoop and is simplified to our objective.
 */
public class HttpServer {
  private static final Log LOG = LogFactory.getLog(HttpServer.class);

  protected final Server webServer;
  protected final Connector listener;
  protected final WebAppContext webAppContext;
  protected final boolean findPort;
  protected final Map<Context, Boolean> defaultContexts =
          new HashMap<>();
  protected final List<String> filterNames = new ArrayList<>();
  private static final int MAX_RETRIES = 10;
  private final boolean listenerStartedExternally;
  static final String STATE_DESCRIPTION_ALIVE = " - alive";
  static final String STATE_DESCRIPTION_NOT_LIVE = " - not live";

  public HttpServer(String name, String bindAddress, int port,
      boolean findPort, Connector connector, Configuration conf,
      String[] pathSpecs) throws IOException {
    this.webServer = new Server();
    this.findPort = findPort;

    if (connector == null) {
      listenerStartedExternally = false;
      listener = createBaseListener(conf);
      listener.setHost(bindAddress);
      listener.setPort(port);

    } else {
      listenerStartedExternally = true;
      listener = connector;
    }
    webServer.addConnector(listener);

    SessionIdManager sessionIdManager = new HashSessionIdManager(new Random(System.currentTimeMillis()));
    webServer.setSessionIdManager(sessionIdManager);

    int maxThreads = conf.getInt("tajo.http.maxthreads", -1);
    // If HTTP_MAX_THREADS is not configured, QueueThreadPool() will use the
    // default value (currently 250).
    QueuedThreadPool threadPool = maxThreads == -1 ? new QueuedThreadPool()
        : new QueuedThreadPool(maxThreads);
    webServer.setThreadPool(threadPool);

    final String appDir = getWebAppsPath(name);
    ContextHandlerCollection contexts = new ContextHandlerCollection();

    webAppContext = new WebAppContext();
    webAppContext.setDisplayName(name);
    webAppContext.setContextPath("/");
    webAppContext.setResourceBase(appDir + "/" + name);
    webAppContext.setDescriptor(appDir + "/" + name + "/WEB-INF/web.xml");

    contexts.addHandler(webAppContext);
    webServer.setHandler(contexts);

    addDefaultApps(contexts, appDir, conf);
  }

  /**
   * Create a required listener for the Jetty instance listening on the port
   * provided. This wrapper and all subclasses must create at least one
   * listener.
   */
  public Connector createBaseListener(Configuration conf) throws IOException {
    return HttpServer.createDefaultChannelConnector();
  }

  static Connector createDefaultChannelConnector() {
    SelectChannelConnector ret = new SelectChannelConnector();
    ret.setLowResourceMaxIdleTime(10000);
    ret.setAcceptQueueSize(128);
    ret.setResolveNames(false);
    ret.setUseDirectBuffers(false);
    return ret;
  }

  /**
   * Add default apps.
   * 
   * @param appDir
   *          The application directory
   * @throws IOException
   */
  protected void addDefaultApps(ContextHandlerCollection parent,
      final String appDir, Configuration conf) throws IOException {
    // set up the context for "/logs/" if "hadoop.log.dir" property is defined.
    String logDir = System.getProperty("tajo.log.dir");
    if (logDir != null) {
      Context logContext = new Context(parent, "/logs");
      logContext.setResourceBase(logDir);
      //logContext.addServlet(AdminAuthorizedServlet.class, "/*");
      logContext.setDisplayName("logs");
      defaultContexts.put(logContext, true);
    }
    // set up the context for "/static/*"
    Context staticContext = new Context(parent, "/static");
    staticContext.setResourceBase(appDir + "/static");
    staticContext.addServlet(DefaultServlet.class, "/*");
    staticContext.setDisplayName("static");
    defaultContexts.put(staticContext, true);
  }
  
  public void addContext(Context ctxt, boolean isFiltered)
      throws IOException {
    webServer.addHandler(ctxt);
    defaultContexts.put(ctxt, isFiltered);
  }
  
  /**
   * Add a context 
   * @param pathSpec The path spec for the context
   * @param dir The directory containing the context
   * @param isFiltered if true, the servlet is added to the filter path mapping 
   * @throws IOException
   */
  protected void addContext(String pathSpec, String dir, boolean isFiltered) throws IOException {
    if (0 == webServer.getHandlers().length) {
      throw new RuntimeException("Couldn't find handler");
    }
    WebAppContext webAppCtx = new WebAppContext();
    webAppCtx.setContextPath(pathSpec);
    webAppCtx.setWar(dir);
    addContext(webAppCtx, true);
  }
  
  /**
   * Set a value in the webapp context. These values are available to the jsp
   * pages as "application.getAttribute(name)".
   * @param name The name of the attribute
   * @param value The value of the attribute
   */
  public void setAttribute(String name, Object value) {
    webAppContext.setAttribute(name, value);
  }
  
  /**
   * Add a servlet in the server.
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   */
  public void addServlet(String name, String pathSpec,
      Class<? extends HttpServlet> clazz) {
    addInternalServlet(name, pathSpec, clazz, false);
    addFilterPathMapping(pathSpec, webAppContext);
  }
  
  /**
   * Add an internal servlet in the server, specifying whether or not to
   * protect with Kerberos authentication. 
   * Note: This method is to be used for adding servlets that facilitate
   * internal communication and not for user facing functionality. For
   * servlets added using this method, filters (except internal Kerberized
   * filters) are not enabled. 
   * 
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   */
  public void addInternalServlet(String name, String pathSpec, 
      Class<? extends HttpServlet> clazz, boolean requireAuth) {
    ServletHolder holder = new ServletHolder(clazz);
    if (name != null) {
      holder.setName(name);
    }
    webAppContext.addServlet(holder, pathSpec);
    
    if(requireAuth && UserGroupInformation.isSecurityEnabled()) {
       LOG.info("Adding Kerberos filter to " + name);
       ServletHandler handler = webAppContext.getServletHandler();
       FilterMapping fmap = new FilterMapping();
       fmap.setPathSpec(pathSpec);
       fmap.setFilterName("krb5Filter");
       fmap.setDispatches(Handler.ALL);
       handler.addFilterMapping(fmap);
    }
  }
  
  /**
   * Add the path spec to the filter path mapping.
   * @param pathSpec The path spec
   * @param webAppCtx The WebApplicationContext to add to
   */
  protected void addFilterPathMapping(String pathSpec,
      Context webAppCtx) {
    ServletHandler handler = webAppCtx.getServletHandler();
    for(String name : filterNames) {
      FilterMapping fmap = new FilterMapping();
      fmap.setPathSpec(pathSpec);
      fmap.setFilterName(name);
      fmap.setDispatches(Handler.ALL);
      handler.addFilterMapping(fmap);
    }
  }
  
  protected String getWebAppsPath(String name) throws FileNotFoundException {
    URL url = getClass().getClassLoader().getResource("webapps/" + name);
    if (url == null) {
      throw new FileNotFoundException("webapps/" + name + " not found in CLASSPATH");
    }
    String urlString = url.toString();
    return urlString.substring(0, urlString.lastIndexOf('/'));
  }
  
  /**
   * Get the value in the webapp context.
   * @param name The name of the attribute
   * @return The value of the attribute
   */
  public Object getAttribute(String name) {
    return webAppContext.getAttribute(name);
  }  
  
  /**
   * Get the port that the server is on
   * @return the port
   */
  public int getPort() {
    return webServer.getConnectors()[0].getLocalPort();
  }

  /**
   * Set the min, max number of worker threads (simultaneous connections).
   */
  public void setThreads(int min, int max) {
    QueuedThreadPool pool = (QueuedThreadPool) webServer.getThreadPool() ;
    pool.setMinThreads(min);
    pool.setMaxThreads(max);
  }

  /**
   * Start the server. Does not wait for the server to start.
   */
  public void start() throws IOException {
    try {
      if (listenerStartedExternally) { // Expect that listener was started
                                       // securely
        if (listener.getLocalPort() == -1) // ... and verify
          throw new Exception("Exepected webserver's listener to be started "
              + "previously but wasn't");
        // And skip all the port rolling issues.
        webServer.start();
      } else {
        int port;
        int oriPort = listener.getPort(); // The original requested port
        while (true) {
          try {
            port = webServer.getConnectors()[0].getLocalPort();
            LOG.debug("Port returned by webServer.getConnectors()[0]."
                + "getLocalPort() before open() is " + port
                + ". Opening the listener on " + oriPort);
            listener.open();
            port = listener.getLocalPort();
            LOG.debug("listener.getLocalPort() returned "
                + listener.getLocalPort()
                + " webServer.getConnectors()[0].getLocalPort() returned "
                + webServer.getConnectors()[0].getLocalPort());
            // Workaround to handle the problem reported in HADOOP-4744
            if (port < 0) {
              Thread.sleep(100);
              int numRetries = 1;
              while (port < 0) {
                LOG.warn("listener.getLocalPort returned " + port);
                if (numRetries++ > MAX_RETRIES) {
                  throw new Exception(" listener.getLocalPort is returning "
                      + "less than 0 even after " + numRetries + " resets");
                }
                for (int i = 0; i < 2; i++) {
                  LOG.info("Retrying listener.getLocalPort()");
                  port = listener.getLocalPort();
                  if (port > 0) {
                    break;
                  }
                  Thread.sleep(200);
                }
                if (port > 0) {
                  break;
                }
                LOG.info("Bouncing the listener");
                listener.close();
                Thread.sleep(1000);
                listener.setPort(oriPort == 0 ? 0 : (oriPort += 1));
                listener.open();
                Thread.sleep(100);
                port = listener.getLocalPort();
              }
            } // Workaround end
            LOG.info("Jetty bound to port " + port);
            webServer.start();
            break;
          } catch (IOException ex) {
            // if this is a bind exception,
            // then try the next port number.
            if (ex instanceof BindException) {
              if (!findPort) {
                BindException be = new BindException("Port in use: "
                    + listener.getHost() + ":" + listener.getPort());
                be.initCause(ex);
                throw be;
              }
            } else {
              LOG.info("HttpServer.start() threw a non Bind IOException");
              throw ex;
            }
          } catch (MultiException ex) {
            LOG.info("HttpServer.start() threw a MultiException");
            throw ex;
          }
          listener.setPort((oriPort += 1));
        }
      }
      // Make sure there is no handler failures.
      Handler[] handlers = webServer.getHandlers();
      for (Handler handler : handlers) {
        if (handler.isFailed()) {
          throw new IOException(
              "Problem in starting http server. Server handlers failed");
        }
      }
    } catch (IOException e) {
      throw e;
    } catch (InterruptedException e) {
      throw (IOException) new InterruptedIOException(
          "Interrupted while starting HTTP server").initCause(e);
    } catch (Exception e) {
      throw new IOException("Problem starting http server", e);
    }
  }

  /**
   * stop the server
   */
  public void stop() throws Exception {
    MultiException exception = null;
    try {
      listener.close();
    } catch (Exception e) {
      LOG.error(
          "Error while stopping listener for webapp"
              + webAppContext.getDisplayName(), e);
      exception = addMultiException(exception, e);
    }

    try {
      // clear & stop webAppContext attributes to avoid memory leaks.
      webAppContext.clearAttributes();
      webAppContext.stop();
    } catch (Exception e) {
      LOG.error("Error while stopping web app context for webapp "
          + webAppContext.getDisplayName(), e);
      exception = addMultiException(exception, e);
    }
    try {
      webServer.stop();
    } catch (Exception e) {
      LOG.error(
          "Error while stopping web server for webapp "
              + webAppContext.getDisplayName(), e);
      exception = addMultiException(exception, e);
    }

    if (exception != null) {
      exception.ifExceptionThrow();
    }

  }

  private MultiException addMultiException(MultiException exception, Exception e) {
    if (exception == null) {
      exception = new MultiException();
    }
    exception.add(e);
    return exception;
  }

  public void join() throws InterruptedException {
    webServer.join();
  }

  /**
   * Test for the availability of the web server
   * 
   * @return true if the web server is started, false otherwise
   */
  public boolean isAlive() {
    return webServer != null && webServer.isStarted();
  }

  /**
   * Return the host and port of the HttpServer, if live
   * 
   * @return the classname and any HTTP URL
   */
  @Override
  public String toString() {
    return listener != null ? ("HttpServer at http://" + listener.getHost()
        + ":" + listener.getLocalPort() + "/" + (isAlive() ? STATE_DESCRIPTION_ALIVE
        : STATE_DESCRIPTION_NOT_LIVE))
        : "Inactive HttpServer";
  }
}
