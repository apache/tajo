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

public class StringUtils {
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
}
