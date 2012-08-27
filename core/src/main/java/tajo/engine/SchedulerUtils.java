package tajo.engine;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * @author jihoon
 */
public class SchedulerUtils {

  public static class MapComparatorBySize implements Comparator<Map>{

    @Override
    public int compare(Map m1, Map m2) {
      return m1.size() - m2.size();
    }
  }

  public static List<Map> sortListOfMapsBySize(
      final List<Map> maplist) {
    Map[] arr = new Map[maplist.size()];
    arr = maplist.toArray(arr);
    Arrays.sort(arr, new MapComparatorBySize());

    List<Map> newlist = Lists.newArrayList(arr);
    return newlist;
  }
}
