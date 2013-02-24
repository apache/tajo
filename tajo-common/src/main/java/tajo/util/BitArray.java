/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.util;

import java.nio.ByteBuffer;

public class BitArray {
  private byte [] data;
  private int length;

  public BitArray(int numBits) {
    data = new byte[numBits];
    this.length = numBits;
  }
  public BitArray(byte [] bytes) {
    this.data = bytes;
    length = bytes.length * 8;
  }

  public void set(int idx) {
    int offset;
    byte dummy;

    if (idx >= length)
      throw new IllegalArgumentException("length is " + length
          + ", but a given index is " + idx + ".");

    offset = idx % 8;
    dummy = 1;
    dummy <<= 7-offset;
    data[idx / 8] |= dummy;
  }

  public boolean get(int idx) {
    int offset;

    if(idx >= length)
      throw new IllegalArgumentException("length is " + length
          + ", but a given index is " + idx + ".");

    offset = idx % 8;
    return (((data[idx / 8] >>> 7 - offset) & 1) == 1);
  }

  public void clear() {
    for (int i = 0; i < data.length; i++) {
      data[i] = 0;
    }
  }

  public int size() {
    return length;
  }

  public byte [] toArray() {
    return data;
  }

  public void fromByteBuffer(ByteBuffer byteBuffer) {
    clear();
    int i = 0;
    while(byteBuffer.hasRemaining()) {
      data[i] = byteBuffer.get();
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();

    for(int i = 0;i < length;i++) {
      if(this.get(i))
        sb.append("1");
      else
        sb.append("0");

      if (i > 0 && i % 8 == 0) {
        sb.append(" ");
      }
    }

    return sb.toString();
  }
}
