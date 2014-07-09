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

package org.apache.tajo.util;

import org.apache.commons.lang.CharUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.SignalLogger;

import java.util.Arrays;

public class StringUtils {

  /**
   * Priority of the StringUtils shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 0;

  /**
   *
   * Given the time in long milliseconds, returns a
   * String in the format X hrs, Y mins, S sec, M msecs
   *
   * @param timeDiff The time difference to format
   */
  public static String formatTime(long timeDiff){
    StringBuilder buf = new StringBuilder();
    long hours = timeDiff / (60*60*1000);
    long rem = (timeDiff % (60*60*1000));
    long minutes =  rem / (60*1000);
    rem = rem % (60*1000);
    long seconds = rem / 1000;

    if (hours != 0){
      buf.append(hours);
      buf.append(" hrs, ");
    }
    if (minutes != 0){
      buf.append(minutes);
      buf.append(" mins, ");
    }

    if (seconds != 0) {
      buf.append(seconds);
      buf.append(" sec");
    }

    if (timeDiff < 1000) {
      buf.append(timeDiff);
      buf.append(" msec");
    }
    return buf.toString();
  }

  public static String quote(String str) {
    return "'" + str + "'";
  }

  public static String doubleQuote(String str) {
    return "\"" + str + "\"";
  }

  public static boolean isPartOfAnsiSQLIdentifier(char character) {
    return
        isLowerCaseAlphabet(character) ||
        isUpperCaseAlphabet(character) ||
        isDigit(character)             ||
        isUndersscore(character);
  }

  public static boolean isUndersscore(char character) {
    return character == '_';
  }

  public static boolean isLowerCaseAlphabet(char character) {
    return 'a' <= character && character <= 'z';
  }

  public static boolean isUpperCaseAlphabet(char character) {
    return 'A' <= character && character <= 'Z';
  }

  public static boolean isDigit(char character) {
    return '0' <= character && character <= '9';
  }

  private static final String REGEX_SPECIAL_CHARACTERS = "([.*${}?|\\^\\-\\[\\]])";
  public static String escapeRegexp(String literal) {
    return literal.replaceAll(REGEX_SPECIAL_CHARACTERS, "\\\\$1");
  }

  private static final String LIKE_SPECIAL_CHARACTERS = "([_%])";
  public static String escapeLike(String literal) {
    return literal.replaceAll(LIKE_SPECIAL_CHARACTERS, "\\\\$1");
  }

  /**
   * Return a message for logging.
   * @param prefix prefix keyword for the message
   * @param msg content of the message
   * @return a message for logging
   */
  private static String toStartupShutdownString(String prefix, String [] msg) {
    StringBuilder b = new StringBuilder(prefix);
    b.append("\n/************************************************************");
    for(String s : msg)
      b.append("\n" + prefix + s);
    b.append("\n************************************************************/");
    return b.toString();
  }

  /**
   * Print a log message for starting up and shutting down
   * @param clazz the class of the server
   * @param args arguments
   * @param LOG the target log object
   */
  public static void startupShutdownMessage(Class<?> clazz, String[] args,
                                            final org.apache.commons.logging.Log LOG) {
    final String hostname = org.apache.hadoop.net.NetUtils.getHostname();
    final String classname = clazz.getSimpleName();
    LOG.info(
        toStartupShutdownString("STARTUP_MSG: ", new String[] {
            "Starting " + classname,
            "  host = " + hostname,
            "  args = " + Arrays.asList(args),
            "  version = " + org.apache.tajo.util.VersionInfo.getVersion(),
            "  classpath = " + System.getProperty("java.class.path"),
            "  build = " + org.apache.tajo.util.VersionInfo.getUrl() + " -r "
                + org.apache.tajo.util.VersionInfo.getRevision()
                + "; compiled by '" + org.apache.tajo.util.VersionInfo.getUser()
                + "' on " + org.apache.tajo.util.VersionInfo.getDate(),
            "  java = " + System.getProperty("java.version") }
        )
    );

    if (SystemUtils.IS_OS_UNIX) {
      try {
        SignalLogger.INSTANCE.register(LOG);
      } catch (Throwable t) {
        LOG.warn("failed to register any UNIX signal loggers: ", t);
      }
    }
    ShutdownHookManager.get().addShutdownHook(
        new Runnable() {
          @Override
          public void run() {
            LOG.info(toStartupShutdownString("SHUTDOWN_MSG: ", new String[]{
                "Shutting down " + classname + " at " + hostname}));
          }
        }, SHUTDOWN_HOOK_PRIORITY);
  }

  public static String unicodeEscapedDelimiter(String value) {
    try {
      String delimiter = StringEscapeUtils.unescapeJava(value);
      return unicodeEscapedDelimiter(delimiter.charAt(0));
    } catch (Throwable e) {
    }
    return value;
  }

  public static String unicodeEscapedDelimiter(char c) {
    return CharUtils.unicodeEscaped(c);
  }
}
