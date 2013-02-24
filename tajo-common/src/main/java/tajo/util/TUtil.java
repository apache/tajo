/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

import tajo.QueryIdFactory;
import tajo.QueryUnitAttemptId;

import java.util.*;

/**
 * It provides miscellaneous and useful util methods.
 * 
 * @author Hyunsik Choi
 */
public class TUtil {
  /**
   * check two objects as equals. 
   * It will return true even if they are all null.
   * 
   * @param s1
   * @param s2
   * @return true if they are equal or all null
   */
  public static boolean checkEquals(Object s1, Object s2) {
    if (s1 == null ^ s2 == null) {
      return false;
    } else if (s1 == null && s2 == null) {
      return true;
    } else {
      return s1.equals(s2);
    }
  }

  /**
   * check two arrays as equals. 
   * It will return true even if they are all null.
   * 
   * @param s1
   * @param s2
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

  public static <T> Set<T> newHashSet() {
    return new HashSet<T>();
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

  public  static QueryUnitAttemptId newQueryUnitAttemptId() {
    return QueryIdFactory.newQueryUnitAttemptId(
        QueryIdFactory.newQueryUnitId(
                QueryIdFactory.newSubQueryId(
                    QueryIdFactory.newQueryId())), 0);
  }
}
