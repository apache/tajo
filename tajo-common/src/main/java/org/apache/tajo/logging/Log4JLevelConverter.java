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

import java.util.HashMap;
import java.util.Map;

/**
 * Description on level conversions can be found in this following link. <br/>
 * <a href="http://logging.apache.org/log4j/2.x/log4j-jul/index.html">Description for level conversions</a>
 */
public class Log4JLevelConverter {

  private static final Map<java.util.logging.Level, org.apache.log4j.Level> JULtoLog4j =
      new HashMap<java.util.logging.Level, org.apache.log4j.Level>(10);
  
  static {
    JULtoLog4j.put(java.util.logging.Level.OFF, org.apache.log4j.Level.OFF);
    JULtoLog4j.put(java.util.logging.Level.SEVERE, org.apache.log4j.Level.FATAL);
    JULtoLog4j.put(java.util.logging.Level.WARNING, org.apache.log4j.Level.WARN);
    JULtoLog4j.put(java.util.logging.Level.INFO, org.apache.log4j.Level.INFO);
    JULtoLog4j.put(java.util.logging.Level.CONFIG, org.apache.log4j.Level.DEBUG);
    JULtoLog4j.put(java.util.logging.Level.FINE, org.apache.log4j.Level.DEBUG);
    JULtoLog4j.put(java.util.logging.Level.FINER, org.apache.log4j.Level.TRACE);
    JULtoLog4j.put(java.util.logging.Level.FINEST, org.apache.log4j.Level.TRACE);
    JULtoLog4j.put(java.util.logging.Level.ALL, org.apache.log4j.Level.ALL);
  }
  
  public static org.apache.log4j.Level getLog4JLevel(java.util.logging.Level julLevel) {
    return JULtoLog4j.get(julLevel);
  }
}
