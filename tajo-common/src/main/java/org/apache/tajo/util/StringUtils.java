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
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.SignalLogger;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;
import java.util.BitSet;

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

  static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder(); // or "ISO-8859-1" for ISO Latin 1

  public static boolean isPureAscii(String v) {
    return asciiEncoder.canEncode(v);
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
      StringBuilder builder = new StringBuilder();
      for (char achar : delimiter.toCharArray()) {
        builder.append(unicodeEscapedDelimiter(achar));
      }
      return builder.toString();
    } catch (Throwable e) {
    }
    return value;
  }

  public static String unicodeEscapedDelimiter(char c) {
    return CharUtils.unicodeEscaped(c);
  }

  /**
   * The following lines of code that deals with escape characters is mostly copied from HIVE's FileUtils.java 
   */

  static BitSet charToEscape = new BitSet(128);
  static {
    for (char c = 0; c < ' '; c++) {
      charToEscape.set(c);
    }

    /**
     * ASCII 01-1F are HTTP control characters that need to be escaped.
     * \u000A and \u000D are \n and \r, respectively.
     */
    char[] clist = new char[] {'\u0001', '\u0002', '\u0003', '\u0004',
        '\u0005', '\u0006', '\u0007', '\u0008', '\u0009', '\n', '\u000B',
        '\u000C', '\r', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012',
        '\u0013', '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019',
        '\u001A', '\u001B', '\u001C', '\u001D', '\u001E', '\u001F',
        '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '\u007F', '{',
        '[', ']', '^'};

    for (char c : clist) {
      charToEscape.set(c);
    }

    if(Shell.WINDOWS){
      // On windows, following chars need to be escaped as well
      char [] winClist = {' ', '<','>','|'};
      for (char c : winClist) {
        charToEscape.set(c);
      }
    }
  }

  static boolean needsEscaping(char c) {
    return c >= 0 && c < charToEscape.size() && charToEscape.get(c);
  }

  public static String escapePathName(String path) {
    return escapePathName(path, null);
  }

  /**
   * Escapes a path name.
   * @param path The path to escape.
   * @param defaultPath
   * The default name for the path, if the given path is empty or null.
   * @return An escaped path name.
   */
  public static String escapePathName(String path, String defaultPath) {
    if (path == null || path.length() == 0) {
      if (defaultPath == null) {
        return "__TAJO_DEFAULT_PARTITION__";
      } else {
        return defaultPath;
      }
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (needsEscaping(c)) {
        sb.append('%');
        sb.append(String.format("%1$02X", (int) c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  public static String unescapePathName(String path) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (c == '%' && i + 2 < path.length()) {
        int code = -1;
        try {
          code = Integer.valueOf(path.substring(i + 1, i + 3), 16);
        } catch (Exception e) {
          code = -1;
        }
        if (code >= 0) {
          sb.append((char) code);
          i += 2;
          continue;
        }
      }
      sb.append(c);
    }
    return sb.toString();
  }

  public static char[][] padChars(char []...bytes) {
    char[] startChars = bytes[0];
    char[] endChars = bytes[1];

    char[][] padded = new char[2][];
    int max = Math.max(startChars.length, endChars.length);

    padded[0] = new char[max];
    padded[1] = new char[max];

    for (int i = 0; i < startChars.length; i++) {
      padded[0][i] = startChars[i];
    }
    for (int i = startChars.length; i < max; i++) {
      padded[0][i] = 0;
    }
    for (int i = 0; i < endChars.length; i++) {
      padded[1][i] = endChars[i];
    }
    for (int i = endChars.length; i < max; i++) {
      padded[1][i] = 0;
    }

    return padded;
  }
  
  public static char[] convertBytesToChars(byte[] src, Charset charset) {
    CharsetDecoder decoder = charset.newDecoder();
    char[] resultArray = new char[(int) (src.length * decoder.maxCharsPerByte())];
    
    if (src.length != 0) {
      ByteBuffer byteBuffer = ByteBuffer.wrap(src);
      CharBuffer charBuffer = CharBuffer.wrap(resultArray);
      
      decoder.onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);
      decoder.reset();
      
      CoderResult coderResult = decoder.decode(byteBuffer, charBuffer, true);
      if (coderResult.isUnderflow()) {
        coderResult = decoder.flush(charBuffer);
        
        if (coderResult.isUnderflow()) {
          if (resultArray.length != charBuffer.position()) {
            resultArray = Arrays.copyOf(resultArray, charBuffer.position());
          }
        }
      }
    }
    
    return resultArray;
  }
  
  public static byte[] convertCharsToBytes(char[] src, Charset charset) {
    CharsetEncoder encoder = charset.newEncoder();
    byte[] resultArray = new byte[(int) (src.length * encoder.maxBytesPerChar())];
    
    if (src.length != 0) {
      CharBuffer charBuffer = CharBuffer.wrap(src);
      ByteBuffer byteBuffer = ByteBuffer.wrap(resultArray);
      
      encoder.onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);
      encoder.reset();
      
      CoderResult coderResult = encoder.encode(charBuffer, byteBuffer, true);
      if (coderResult.isUnderflow()) {
        coderResult = encoder.flush(byteBuffer);
        
        if (coderResult.isUnderflow()) {
          if (resultArray.length != byteBuffer.position()) {
            resultArray = Arrays.copyOf(resultArray, byteBuffer.position());
          }
        }
      }
    }
    
    return resultArray;
  }

  /**
   * Concatenate all objects' string with a delimiter string
   *
   * @param objects Iterable objects
   * @param delimiter Delimiter string
   * @return A joined string
   */
  public static String join(Iterable objects, String delimiter) {
    boolean first = true;
    StringBuilder sb = new StringBuilder();
    for(Object object : objects) {
      if (first) {
        first = false;
      } else {
        sb.append(delimiter);
      }

      sb.append(object.toString());
    }

    return sb.toString();
  }

  /**
   * Concatenate all objects' string with the delimiter ", "
   *
   * @param objects Iterable objects
   * @return A joined string
   */
  public static String join(Object[] objects) {
    return join(objects, ", ", 0, objects.length);
  }

  /**
   * Concatenate all objects' string with a delimiter string
   *
   * @param objects object array
   * @param delimiter Delimiter string
   * @param startIndex the begin index to join
   * @return A joined string
   */
  public static String join(Object[] objects, String delimiter, int startIndex) {
    return join(objects, delimiter, startIndex, objects.length);
  }

  /**
   * Concatenate all objects' string with a delimiter string
   *
   * @param objects object array
   * @param delimiter Delimiter string
   * @param startIndex the begin index to join
   * @param length the number of columns to be joined
   * @return A joined string
   */
  public static String join(Object[] objects, String delimiter, int startIndex, int length) {
    boolean first = true;
    StringBuilder sb = new StringBuilder();
    for(int i = startIndex; i + startIndex < length; i++) {
      if (first) {
        first = false;
      } else {
        sb.append(delimiter);
      }

      sb.append(objects[i].toString());
    }

    return sb.toString();
  }
}
