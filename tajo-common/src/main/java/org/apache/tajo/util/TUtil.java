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

import com.google.common.base.Objects;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * It provides miscellaneous and useful util methods.
 */
public class TUtil {
  /**
   * check two objects as equals.
   * It will return true even if they are all null.
   *
   * @param s1 the first object to be compared.
   * @param s2 the second object to be compared
   *
   * @return true if they are equal or all null
   */
  public static boolean checkEquals(Object s1, Object s2) {
    return Objects.equal(s1, s2);
  }

  /**
   * check two arrays as equals. It also check the equivalence of null.
   * It will return true even if they are all null.
   *
   * @param s1 the first array to be compared.
   * @param s2 the second array to be compared
   * @return true if they are equal or all null
   */
  public static boolean checkEquals(Object [] s1, Object [] s2) {
    if (s1 == null ^ s2 == null) {
      return false;
    } else if (s1 == null && s2 == null) {
      return true;
    } else {
      return Arrays.equals(s1, s2);
    }
  }

  public static <T> T[] concat(T[] first, T[] second) {
    T[] result = Arrays.copyOf(first, first.length + second.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }

  public static <T> T[] concatAll(T[] first, T[]... rest) {
    int totalLength = first.length;
    for (T[] array : rest) {
      totalLength += array.length;
    }
    T[] result = Arrays.copyOf(first, totalLength);
    int offset = first.length;
    for (T[] array : rest) {
      System.arraycopy(array, 0, result, offset, array.length);
      offset += array.length;
    }
    return result;
  }

  public static <T> Set<T> newHashSet() {
    return new HashSet<T>();
  }

  public static <T> Set<T> newHashSet(T ...items) {
    return new HashSet<T>(Arrays.asList(items));
  }

  public static <K,V> Map<K,V> newHashMap() {
    return new HashMap<K, V>();
  }

  public static <K,V> Map<K,V> newHashMap(Map<K,V> map) {
    return new HashMap<K, V>(map);
  }

  public static <K, V> Map<K,V> newHashMap(K k, V v) {
    HashMap<K, V> newMap = new HashMap<K, V>();
    newMap.put(k, v);
    return newMap;
  }

  public static <K,V> Map<K,V> newLinkedHashMap() {
    return new LinkedHashMap<K, V>();
  }

  public static <K, V> Map<K,V> newLinkedHashMap(K k, V v) {
    HashMap<K, V> newMap = new LinkedHashMap<K, V>();
    newMap.put(k, v);
    return newMap;
  }

  public static <K,V> Map<K,V> newConcurrentHashMap() {
    return new ConcurrentHashMap<K, V>();
  }

  public static <T> List<T> newList() {
    return new ArrayList<T>();
  }

  public static <T> List<T> newList(T...items) {
    List<T> list = new ArrayList<T>();
    for (T t : items) {
      list.add(t);
    }

    return list;
  }

  public static <T> List<T> newList(Collection<T> items) {
    List<T> list = new ArrayList<T>();
    for (T t : items) {
      list.add(t);
    }

    return list;
  }

  /**
   * It check if T is null or not.
   *
   * @param reference the object reference to be checked
   * @param <T> The object type
   * @return The reference
   */
  public static <T> T checkNotNull(T reference) {
    if (reference == null) {
      throw new NullPointerException();
    }
    return reference;
  }

  public static <KEY1, VALUE> void putToNestedList(Map<KEY1, List<VALUE>> map, KEY1 k1, VALUE value) {
    if (map.containsKey(k1)) {
      map.get(k1).add(value);
    } else {
      map.put(k1, TUtil.newList(value));
    }
  }

  public static <KEY1, VALUE> void putCollectionToNestedList(Map<KEY1, List<VALUE>> map, KEY1 k1,
                                                             Collection<VALUE> list) {
    if (map.containsKey(k1)) {
      map.get(k1).addAll(list);
    } else {
      map.put(k1, TUtil.newList(list));
    }
  }

  public static <KEY1, KEY2, VALUE> void putToNestedMap(Map<KEY1, Map<KEY2, VALUE>> map, KEY1 k1, KEY2 k2,
                                                        VALUE value) {
    if (map.containsKey(k1)) {
      map.get(k1).put(k2, value);
    } else {
      map.put(k1, TUtil.newLinkedHashMap(k2, value));
    }
  }

  /**
   * It checks if an item is included in Map<KEY1, Map<KEY2, VALUE>>.
   *
   * @return True if the item is included in the nested map.
   */
  public static <KEY1, KEY2, VALUE> boolean containsInNestedMap(Map<KEY1, Map<KEY2, VALUE>> map, KEY1 k1, KEY2 k2) {
    return map.containsKey(k1) && map.get(k1).containsKey(k2);
  }

  /**
   * Get an item in Map<KEY1, Map<KEY2, VALUE>>.
   *
   * @return a VALUE
   */
  public static <KEY1, KEY2, VALUE> VALUE getFromNestedMap(Map<KEY1, Map<KEY2, VALUE>> map, KEY1 k1, KEY2 k2) {
    if (map.containsKey(k1)) {
      return map.get(k1).get(k2);
    } else {
      return null;
    }
  }

  public static String collectionToString(Collection objects, String delimiter) {
    boolean first = true;
    StringBuilder sb = new StringBuilder();
    for(Object object : objects) {
      if (first) {
        first = false;
      } else {
        sb.append(delimiter);
      }

      sb.append(object.toString());
    }

    return sb.toString();
  }

  public static String arrayToString(Object [] objects) {
    boolean first = true;
    StringBuilder sb = new StringBuilder();
    for(Object object : objects) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }

      sb.append(object.toString());
    }

    return sb.toString();
  }

  public static <T> T [] toArray(Collection<T> collection, Class<T> type) {
    T array = (T) Array.newInstance(type, collection.size());
    return collection.toArray((T[]) array);
  }

  public static int[] toArray(Collection<Integer> collection) {
    int[] array = new int[collection.size()];

    int index = 0;
    for (Integer eachInt: collection) {
      array[index++] = eachInt;
    }

    return array;
  }

  /**
   * It returns the exact code point at which this running thread is executed.
   *
   * @param depth in the call stack (0 means current method, 1 means call method, ...)
   * @return A string including class name, method, and line.
   */
  public static String getCurrentCodePoint(final int depth) {
    final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
    StackTraceElement element = ste[2 + depth];
    return element.getClassName() + ":" + element.getMethodName() + "(" + element.getLineNumber() +")";
  }
}
