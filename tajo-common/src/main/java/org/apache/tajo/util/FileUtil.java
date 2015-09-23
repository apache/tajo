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

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.conf.TajoConf;

import java.io.*;
import java.nio.charset.Charset;

/**
 * Simple File Utilities
 */
public class FileUtil {

  public static String readTextFile(File file) throws IOException {
    StringBuilder fileData = new StringBuilder(1000);
    BufferedReader reader = new BufferedReader(new FileReader(file));
    char[] buf = new char[1024];
    int numRead;
    try {
      while ((numRead = reader.read(buf)) != -1) {
        String readData = String.valueOf(buf, 0, numRead);
        fileData.append(readData);
        buf = new char[1024];
      }
    } finally {
      IOUtils.cleanup(null, reader);
    }
    return fileData.toString();
  }

  /**
   * Write a string into a file
   *
   * @param text
   * @param path File path
   * @throws IOException
   */
  public static void writeTextToFile(String text, Path path) throws IOException {
    FileSystem fs = path.getFileSystem(new TajoConf());
    if (!fs.exists(path.getParent())) {
      fs.mkdirs(path.getParent());
    }
    FSDataOutputStream out = fs.create(path);
    out.write(text.getBytes());
    out.close();
  }

  public static String readTextFromStream(InputStream inputStream)
      throws IOException {
    try {
      StringBuilder fileData = new StringBuilder(1000);
      byte[] buf = new byte[1024];
      int numRead;
      while ((numRead = inputStream.read(buf)) != -1) {
        String readData = new String(buf, 0, numRead, Charset.defaultCharset());
        fileData.append(readData);
      }
      return fileData.toString();
    } finally {
      IOUtils.closeStream(inputStream);
    }
  }

  public static void writeTextToStream(String text, OutputStream outputStream) throws IOException {
    try {
      outputStream.write(text.getBytes());
    } finally {
      IOUtils.closeStream(outputStream);
    }
  }

  public static String humanReadableByteCount(long bytes, boolean si) {
    int unit = si ? 1000 : 1024;
    if (bytes < unit) return bytes + " B";
    int exp = (int) (Math.log(bytes) / Math.log(unit));
    String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
  }


  /**
   * Close the Closeable objects and <b>ignore</b> any {@link IOException} or
   * null pointers. Must only be used for cleanup in exception handlers.
   *
   * @param log the log to record problems to at debug level. Can be null.
   * @param closeables the objects to close
   */
  public static void cleanup(Log log, java.io.Closeable... closeables) {
    for (java.io.Closeable c : closeables) {
      if (c != null) {
        try {
          c.close();
        } catch(IOException e) {
          if (log != null && log.isDebugEnabled()) {
            log.debug("Exception in closing " + c, e);
          }
        }
      }
    }
  }
}
