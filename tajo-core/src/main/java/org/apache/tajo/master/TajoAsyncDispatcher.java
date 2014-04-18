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

package org.apache.tajo.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TajoAsyncDispatcher extends AbstractService  implements Dispatcher {

  private static final Log LOG = LogFactory.getLog(TajoAsyncDispatcher.class);

  private final BlockingQueue<Event> eventQueue;
  private volatile boolean stopped = false;

  private Thread eventHandlingThread;
  protected final Map<Class<? extends Enum>, EventHandler> eventDispatchers;
  private boolean exitOnDispatchException;

  private String id;

  public TajoAsyncDispatcher(String id) {
    this(id, new LinkedBlockingQueue<Event>());
  }

  public TajoAsyncDispatcher(String id, BlockingQueue<Event> eventQueue) {
    super(TajoAsyncDispatcher.class.getName());
    this.id = id;
    this.eventQueue = eventQueue;
    this.eventDispatchers = new HashMap<Class<? extends Enum>, EventHandler>();
  }

  Runnable createThread() {
    return new Runnable() {
      @Override
      public void run() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          Event event;
          try {
            event = eventQueue.take();
            if(LOG.isDebugEnabled()) {
              LOG.debug(id + ",event take:" + event.getType() + "," + event);
            }
          } catch(InterruptedException ie) {
            if (!stopped) {
              LOG.warn("AsyncDispatcher thread interrupted");
            }
            return;
          }
          dispatch(event);
        }
      }
    };
  }

  @Override
  public synchronized void init(Configuration conf) {
    this.exitOnDispatchException =
        conf.getBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY,
            Dispatcher.DEFAULT_DISPATCHER_EXIT_ON_ERROR);
    super.init(conf);
  }

  @Override
  public void start() {
    //start all the components
    super.start();
    eventHandlingThread = new Thread(createThread());
    eventHandlingThread.setName("AsyncDispatcher event handler");
    eventHandlingThread.start();

    LOG.info("AsyncDispatcher started:" + id);
  }

  @Override
  public synchronized void stop() {
    if(stopped) {
      return;
    }
    stopped = true;
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
      try {
        eventHandlingThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted Exception while stopping");
      }
    }

    // stop all the components
    super.stop();

    LOG.info("AsyncDispatcher stopped:" + id);
  }

  @SuppressWarnings("unchecked")
  protected void dispatch(Event event) {
    //all events go thru this loop
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dispatching the event " + event.getClass().getName() + "."
          + event.toString());
    }
    Class<? extends Enum> type = event.getType().getDeclaringClass();

    try{
      EventHandler handler = eventDispatchers.get(type);
      if(handler != null) {
        handler.handle(event);
      } else {
        throw new Exception("No handler for registered for " + type);
      }
    } catch (Throwable t) {
      //TODO Maybe log the state of the queue
      LOG.fatal("Error in dispatcher thread:" + event.getType(), t);
      if (exitOnDispatchException && (ShutdownHookManager.get().isShutdownInProgress()) == false) {
        LOG.info("Exiting, bye..");
        System.exit(-1);
      }
    } finally {
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void register(Class<? extends Enum> eventType,
                       EventHandler handler) {
    /* check to see if we have a listener registered */
    EventHandler<Event> registeredHandler = (EventHandler<Event>)
        eventDispatchers.get(eventType);
    LOG.debug("Registering " + eventType + " for " + handler.getClass());
    if (registeredHandler == null) {
      eventDispatchers.put(eventType, handler);
    } else if (!(registeredHandler instanceof MultiListenerHandler)){
      /* for multiple listeners of an event add the multiple listener handler */
      MultiListenerHandler multiHandler = new MultiListenerHandler();
      multiHandler.addHandler(registeredHandler);
      multiHandler.addHandler(handler);
      eventDispatchers.put(eventType, multiHandler);
    } else {
      /* already a multilistener, just add to it */
      MultiListenerHandler multiHandler
          = (MultiListenerHandler) registeredHandler;
      multiHandler.addHandler(handler);
    }
  }

  @Override
  public EventHandler getEventHandler() {
    return new GenericEventHandler();
  }

  class GenericEventHandler implements EventHandler<Event> {
    public void handle(Event event) {
      /* all this method does is enqueue all the events onto the queue */
      int qSize = eventQueue.size();
      if (qSize !=0 && qSize %1000 == 0) {
        LOG.info("Size of event-queue is " + qSize);
      }
      int remCapacity = eventQueue.remainingCapacity();
      if (remCapacity < 1000) {
        LOG.warn("Very low remaining capacity in the event-queue: "
            + remCapacity);
      }
      try {
        if(LOG.isDebugEnabled()) {
          LOG.debug(id + ",add event:" +
              event.getType() + "," + event + "," +
              (eventHandlingThread == null ? "null" : eventHandlingThread.isAlive()));
        }
        eventQueue.put(event);
      } catch (InterruptedException e) {
        if (!stopped) {
          LOG.warn("AsyncDispatcher thread interrupted", e);
        }
        throw new YarnRuntimeException(e);
      }
    }
  }

  /**
   * Multiplexing an event. Sending it to different handlers that
   * are interested in the event.
   */
  static class MultiListenerHandler implements EventHandler<Event> {
    List<EventHandler<Event>> listofHandlers;

    public MultiListenerHandler() {
      listofHandlers = new ArrayList<EventHandler<Event>>();
    }

    @Override
    public void handle(Event event) {
      for (EventHandler<Event> handler: listofHandlers) {
        handler.handle(event);
      }
    }

    void addHandler(EventHandler<Event> handler) {
      listofHandlers.add(handler);
    }

  }
}
