package nta.engine.utils;

import java.util.Arrays;

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
}
