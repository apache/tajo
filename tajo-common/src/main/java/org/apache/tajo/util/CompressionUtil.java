/*
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

import org.apache.tajo.TajoProtos.CodecType;
import org.apache.tajo.exception.UnsupportedException;
import org.iq80.snappy.Snappy;

import java.io.IOException;

public class CompressionUtil {

  public static byte[] compress(CodecType type, byte[] uncompressed) throws IOException {
    switch (type) {
    case SNAPPY:
      return SnappyCodec.compress(uncompressed);
    default:
      throw new IOException(new UnsupportedException("Cannot support " + type));
    }
  }

  public static byte[] decompress(CodecType type, byte[] compressed) throws IOException {
    switch (type) {
    case SNAPPY:
      return SnappyCodec.uncompress(compressed);
    default:
      throw new IOException(new UnsupportedException("Cannot support " + type));
    }
  }

  public static int maxCompressedLength(CodecType type, int byteSize) throws IOException {
    switch (type) {
    case SNAPPY:
      return SnappyCodec.maxCompressedLength(byteSize);
    default:
      throw new IOException(new UnsupportedException("Cannot support " + type));
    }
  }

  /**
   * Java snappy codec
   */
  static class SnappyCodec {

    static byte[] compress(byte[] uncompressed) throws IOException {
      return Snappy.compress(uncompressed);
    }

    static byte[] uncompress(byte[] compressed) throws IOException {
      return Snappy.uncompress(compressed, 0, compressed.length);
    }

    static int maxCompressedLength(int byteSize) throws IOException {
      return Snappy.maxCompressedLength(byteSize);
    }
  }
}
