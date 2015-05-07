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

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;

/**
 * Initiate JUL handlers by using pre-defined default properties.
 */
public class JULDefaultConfiguration {
  
  private static final String defaultLoggingPropertiesName = "jul-logging.properties";

  public JULDefaultConfiguration() throws SecurityException, IOException {
    InputStream configFileInputStream = null;
    
    try {
      configFileInputStream = ClassLoader.getSystemClassLoader()
          .getResourceAsStream(defaultLoggingPropertiesName);
      LogManager.getLogManager().readConfiguration(configFileInputStream);
    } catch (SecurityException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    } finally {
      if (configFileInputStream != null) {
        configFileInputStream.close();
      }
    }
  }
}
