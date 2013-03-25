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

package tajo.storage.hcfile;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

public class Index<T> {
  private List<IndexItem<T>> index = Lists.newArrayList();

  public Index() {

  }

  public void add(IndexItem<T> item) {
    index.add(item);
  }

  public void sort() {
    IndexItem<T>[] array = index.toArray(new IndexItem[index.size()]);
    index.clear();
    Arrays.sort(array);
    for (IndexItem<T> item : array) {
      index.add(item);
    }
  }

  public IndexItem<T> get(int i) {
    return index.get(i);
  }

  public void clear() {
    index.clear();
  }

  public int findPos(IndexItem<T> searchItem) {
    for (int i = 0; i < index.size(); i++) {
      if (index.get(i).equals(searchItem)) {
        return i;
      }
    }
    return -1;
  }

  public int size() {
    return index.size();
  }

  public List<IndexItem<T>> get() {
    return index;
  }

  /**
   * For a given rid, search the index item
   * @param rid search rid
   * @return If the rid is in the index, return the index item which has the rid.
   * Else, return null
   */
  public IndexItem<T> searchExact(long rid) {
    if (index.isEmpty()) {
      return null;
    }

    return searchExact(rid, 0, index.size());
  }

  private IndexItem<T> searchExact(long rid, int start, int length) {
    int leftLength = length/2;
    IndexItem<T> mid = index.get(start + leftLength);
    if (mid.getRid() == rid) {
      return mid;
    } else if (length == 1) {
      return null;
    }

    if (mid.getRid() > rid) {
      return searchExact(rid, start, leftLength);
    } else if (mid.getRid() < rid) {
      return searchExact(rid, start + leftLength, length - leftLength);
    }
    return null;
  }

  /**
   * For a given rid,
   * search the index item of which rid is the largest smaller than the given rid.
   *
   * @param rid  search rid
   * @return If the given rid is in the index, return the index item which has the rid.
   * Else, return the index item which has the largest rid smaller than the given rid.
   */
  public IndexItem<T> searchLargestSmallerThan(long rid) {
    if (index.isEmpty()) {
      return null;
    }
    return searchLargestSmallerThan(rid, 0, index.size());
  }

  private IndexItem<T> searchLargestSmallerThan(long rid, int start, int length) {
    int leftLength = length/2;
    IndexItem<T> mid = index.get(start + leftLength);
    if (mid.getRid() == rid) {
      return mid;
    } else if (length == 1) {
      return index.get(start+leftLength);
    }

    if (mid.getRid() > rid) {
      return searchLargestSmallerThan(rid, start, leftLength);
    } else if (mid.getRid() < rid) {
      return searchLargestSmallerThan(rid, start + leftLength, length - leftLength);
    }

    return null;
  }

  /**
   * For a given rid,
   * search the index item of which rid is the smallest larger than the given rid.
   *
   * @param rid  search rid
   * @return If the given rid is in the index, return the index item which has the rid.
   * Else, return the index item which has the smallest rid larger than the given rid.
   */
  public IndexItem<T> searchSmallestLargerThan(long rid) {
    if (index.isEmpty()) {
      return null;
    }
    return searchSmallestLargerThan(rid, 0, index.size());
  }

  private IndexItem<T> searchSmallestLargerThan(long rid, int start, int length) {
    int leftLength = length/2;
    IndexItem<T> mid = index.get(start + leftLength);
    if (mid.getRid() == rid) {
      return mid;
    } else if (length == 1) {
      if (start+leftLength+1 >= index.size()) {
        return null;
      } else {
        return index.get(start+leftLength+1);
      }
    }

    if (mid.getRid() > rid) {
      return searchSmallestLargerThan(rid, start, leftLength);
    } else if (mid.getRid() < rid) {
      return searchSmallestLargerThan(rid, start + leftLength, length - leftLength);
    }
    return null;
  }
}
