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

package org.apache.tajo.logging;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Properties;
import java.util.logging.ErrorManager;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import org.apache.log4j.Category;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

/**
 * Simple handlers for converting JUL logging events to log4j logging events.
 */
public class Log4JHandler extends Handler {
  
  private static final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
  private static final Properties emptyProperties = new Properties();
  
  public Log4JHandler() {
  }
  
  private String getThreadName(long threadId) {
    ThreadInfo threadInfo = null;
    String threadName;
    
    try {
      threadInfo = threadMXBean.getThreadInfo(threadId);
      threadName = threadInfo.getThreadName();
    } catch (Throwable e) {
      threadName = "thread-" + threadId;
    }
    
    return threadName;
  }
  
  private LoggingEvent convertLoggingEvent(LogRecord logRecord) {
    String loggerName = logRecord.getLoggerName();
    Logger logger = LogManager.getLogger(logRecord.getLoggerName());
    ThrowableInformation throwableInformation = null;
    LocationInfo locationInfo = null;
    String localizedMessage = null;
    
    if (logRecord.getThrown() != null) {
      throwableInformation = new ThrowableInformation(logRecord.getThrown());
    }
    
    try {
      if (getFormatter() != null) {
        localizedMessage = getFormatter().format(logRecord);
      } else {
        localizedMessage = logRecord.getMessage();
      }
    } catch (Exception e) {
      reportError(null, e, ErrorManager.FORMAT_FAILURE);
    }
    
    locationInfo = new LocationInfo(null, 
        logRecord.getSourceClassName(), 
        logRecord.getSourceMethodName(), 
        null);
    
    return new LoggingEvent(loggerName, 
        logger, 
        logRecord.getMillis(), 
        Log4JLevelConverter.getLog4JLevel(logRecord.getLevel()), 
        localizedMessage,
        getThreadName(logRecord.getThreadID()),
        throwableInformation, 
        null,
        locationInfo,
        emptyProperties);
  }

  @Override
  public void publish(LogRecord record) {
    LoggingEvent loggingEvent = convertLoggingEvent(record);
    Category logger = loggingEvent.getLogger();
    
    if (loggingEvent.getLevel().isGreaterOrEqual(logger.getEffectiveLevel())) {
      logger.callAppenders(loggingEvent);
    }
  }

  @Override
  public void flush() {
    
  }

  @Override
  public void close() throws SecurityException {
    
  }

}
