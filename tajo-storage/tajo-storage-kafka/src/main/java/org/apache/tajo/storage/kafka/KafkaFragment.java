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

package org.apache.tajo.storage.kafka;

import org.apache.tajo.storage.fragment.BuiltinFragmentKinds;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.kafka.KafkaFragment.KafkaFragmentKey;

import java.net.URI;

import com.google.common.base.Objects;

/**
 * Fragment for Kafka
 */
public class KafkaFragment extends Fragment<KafkaFragmentKey> {
  private String topicName;
  private boolean last;

  public KafkaFragment(URI uri, String tableName, String topicName, long startOffset, long lastOffset,
      int partitionId, String leaderHost) {
    this(uri, tableName, topicName, startOffset, lastOffset, partitionId, leaderHost, false);
  }

  public KafkaFragment(URI uri, String tableName, String topicName, long startOffset, long lastOffset,
      int partitionId, String leaderHost, boolean last) {
    super(BuiltinFragmentKinds.KAFKA, uri, tableName, new KafkaFragmentKey(partitionId, startOffset),
        new KafkaFragmentKey(partitionId, lastOffset), lastOffset - startOffset, new String[] { leaderHost });
    this.topicName = topicName;
    this.last = last;
  }

  @Override
  public boolean isEmpty() {
    return startKey.isEmpty() || endKey.isEmpty();
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    KafkaFragment frag = (KafkaFragment) super.clone();
    frag.topicName = topicName;
    frag.last = last;
    return frag;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof KafkaFragment) {
      KafkaFragment t = (KafkaFragment) o;
      if (inputSourceId.equals(t.inputSourceId) && topicName.equals(t.topicName)
        && getStartKey().equals(t.getStartKey()) && getEndKey().equals(t.getEndKey())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(inputSourceId, topicName, getStartKey(), getEndKey());
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("\"fragment\": {\"topicName\":");
    builder.append(topicName);
    builder.append(", \"uri\":");
    builder.append(uri);
    builder.append(", \"inputSourceId\":");
    builder.append(inputSourceId);
    builder.append(", \"startKey\":");
    builder.append(startKey);
    builder.append(", \"endKey\":");
    builder.append(endKey);
    builder.append(", \"length\":");
    builder.append(length);
    builder.append("}");
    return builder.toString();
  }

  public boolean isLast() {
    return last;
  }

  public void setLast(boolean last) {
    this.last = last;
  }

  public String getTopicName() {
    return this.topicName;
  }

  public void setStartKey(int partitionId, long startOffset) {
    this.startKey = new KafkaFragmentKey(partitionId, startOffset);
  }

  public void setEndKey(int partitionId, long lastOffset) {
    this.endKey = new KafkaFragmentKey(partitionId, lastOffset);
  }

  public int getPartitionId() {
    return this.startKey.getPartitionId();
  }

  public static class KafkaFragmentKey implements Comparable<KafkaFragmentKey> {
    private final Integer partitionId;
    private final Long offset;

    public KafkaFragmentKey(Integer partitionId, Long offset) {
      this.partitionId = partitionId;
      this.offset = offset;
    }

    public Integer getPartitionId() {
      return partitionId;
    }

    public Long getOffset() {
      return offset;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((offset == null) ? 0 : offset.hashCode());
      result = prime * result + ((partitionId == null) ? 0 : partitionId.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      KafkaFragmentKey other = (KafkaFragmentKey) obj;
      if (offset == null) {
        if (other.offset != null)
          return false;
      } else if (!offset.equals(other.offset))
        return false;
      if (partitionId == null) {
        if (other.partitionId != null)
          return false;
      } else if (!partitionId.equals(other.partitionId))
        return false;
      return true;
    }

    @Override
    public int compareTo(KafkaFragmentKey o) {
      if (partitionId == null || offset == null) {
        return 1;
      }

      if (o.partitionId == null || o.offset == null) {
        return -1;
      }

      int result = partitionId.compareTo(o.partitionId);
      if (result == 0) {
        result = offset.compareTo(o.offset);
      }
      return result;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("{\"partitionId\":");
      builder.append(partitionId);
      builder.append(", \"offset\":");
      builder.append(offset);
      builder.append("}");
      return builder.toString();
    }

    public boolean isEmpty() {
      return partitionId == null || offset == null;
    }
  }
}
