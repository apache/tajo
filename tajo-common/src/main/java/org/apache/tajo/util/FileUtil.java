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

import com.google.protobuf.Message;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;

public class FileUtil {
  public static void writeProto(File file, Message proto) throws IOException {
    FileOutputStream stream = null;
    try {
      stream = new FileOutputStream(file);
      stream.write(proto.toByteArray());
    } finally {
      IOUtils.closeStream(stream);
    }
  }

  public static void writeProto(OutputStream out, Message proto) throws IOException {
    out.write(proto.toByteArray());
  }

  public static void writeProto(FileSystem fs, Path path, Message proto) throws IOException {
    FSDataOutputStream stream = fs.create(path);
    try {
      stream.write(proto.toByteArray());
    } finally {
      IOUtils.closeStream(stream);
    }
  }

  public static Message loadProto(File file, Message proto) throws IOException {
    FileInputStream in = null;
    try {
      in = new FileInputStream(file);
      Message.Builder builder = proto.newBuilderForType().mergeFrom(in);
      return builder.build();
    } finally {
      IOUtils.closeStream(in);
    }
  }

  public static Message loadProto(InputStream in, Message proto) throws IOException {
    Message.Builder builder = proto.newBuilderForType().mergeFrom(in);
    return builder.build();
  }

  public static Message loadProto(FileSystem fs,
                                  Path path, Message proto) throws IOException {
    FSDataInputStream in = null;
    try {
      in = new FSDataInputStream(fs.open(path));
      Message.Builder builder = proto.newBuilderForType().mergeFrom(in);
      return builder.build();
    } finally {
      IOUtils.closeStream(in);
    }
  }

  public static File getFile(String path) {
    return new File(path);
  }

  public static URL getResourcePath(String resource) throws IOException {
    return ClassLoader.getSystemResource(resource);
  }

  public static String readTextFileFromResource(String resource) throws IOException {
    return readTextFromStream(ClassLoader.getSystemResourceAsStream(resource));
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

  public static void writeTextToStream(String text, OutputStream outputStream)
      throws IOException {
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

  public static boolean isLocalPath(Path path) {
    return path.toUri().getScheme().equals("file");
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
