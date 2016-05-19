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

package org.apache.tajo.storage.hbase;

import com.google.common.base.Objects;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.storage.fragment.BuiltinFragmentKinds;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.hbase.HBaseFragment.HBaseFragmentKey;

import java.net.URI;

/**
 * Fragment for HBase
 */
public class HBaseFragment extends Fragment<HBaseFragmentKey> {
  private String hbaseTableName;
  private boolean last;

  public HBaseFragment(URI uri, String tableName, String hbaseTableName, byte[] startRow, byte[] stopRow,
                       String regionLocation) {
    super(BuiltinFragmentKinds.HBASE, uri, tableName, new HBaseFragmentKey(startRow), new HBaseFragmentKey(stopRow),
        TajoConstants.UNKNOWN_LENGTH, new String[]{regionLocation});

    this.hbaseTableName = hbaseTableName;
    this.last = false;
  }

  public HBaseFragment(URI uri, String tableName, String hbaseTableName, byte[] startRow, byte[] stopRow,
                       String regionLocation, boolean last) {
    this(uri, tableName, hbaseTableName, startRow, stopRow, regionLocation);
    this.last = last;
  }

  @Override
  public boolean isEmpty() {
    return startKey.isEmpty() || endKey.isEmpty();
  }

  public void setLength(long length) {
    this.length = length;
  }

  public Object clone() throws CloneNotSupportedException {
    HBaseFragment frag = (HBaseFragment) super.clone();
    frag.hbaseTableName = hbaseTableName;
    frag.last = last;
    return frag;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof HBaseFragment) {
      HBaseFragment t = (HBaseFragment) o;
      if (inputSourceId.equals(t.inputSourceId)
          && startKey.equals(t.startKey)
          && endKey.equals(t.endKey)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(inputSourceId, hbaseTableName, startKey, endKey);
  }

  @Override
  public String toString() {
    return
        "\"fragment\": {\"uri:\"" + uri.toString() +"\", \"tableName\": \""+ inputSourceId +
            "\", hbaseTableName\": \"" + hbaseTableName + "\"" +
            ", \"startRow\": \"" + new String(startKey.bytes) + "\"" +
            ", \"stopRow\": \"" + new String(endKey.bytes) + "\"" +
            ", \"length\": \"" + length + "\"}" ;
  }

  public boolean isLast() {
    return last;
  }

  public void setLast(boolean last) {
    this.last = last;
  }

  public String getHbaseTableName() {
    return hbaseTableName;
  }

  public void setStartRow(byte[] startRow) {
    this.startKey = new HBaseFragmentKey(startRow);
  }

  public void setStopRow(byte[] stopRow) {
    this.endKey = new HBaseFragmentKey(stopRow);
  }

  public static class HBaseFragmentKey implements Comparable<HBaseFragmentKey> {
    private final byte[] bytes;

    public HBaseFragmentKey(byte[] key) {
      this.bytes = key;
    }

    public byte[] getBytes() {
      return bytes;
    }

    @Override
    public int hashCode() {
      return Bytes.hashCode(bytes);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof HBaseFragmentKey) {
        HBaseFragmentKey other = (HBaseFragmentKey) o;
        return Bytes.equals(bytes, other.bytes);
      }
      return false;
    }

    @Override
    public int compareTo(HBaseFragmentKey o) {
      return Bytes.compareTo(bytes, o.bytes);
    }

    @Override
    public String toString() {
      return new String(bytes);
    }

    public boolean isEmpty() {
      return this.bytes == null;
    }
  }
}
