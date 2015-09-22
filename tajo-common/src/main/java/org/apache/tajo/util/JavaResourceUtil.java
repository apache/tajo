/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * An utility for Java resources
 */
public class JavaResourceUtil {

  public static URL getResourceURL(String resource) throws IOException {
    return ClassLoader.getSystemResource(resource);
  }

  /**
   * Read a file stored in a local file system and return the string contents.
   *
   * @param resource Resource file name
   * @return String contents
   * @throws IOException
   */
  public static String readTextFromResource(String resource) throws IOException {
    InputStream stream = ClassLoader.getSystemResourceAsStream(resource);
    if (stream != null) {
      return FileUtil.readTextFromStream(stream);
    } else {
      throw new FileNotFoundException(resource);
    }
  }
}
