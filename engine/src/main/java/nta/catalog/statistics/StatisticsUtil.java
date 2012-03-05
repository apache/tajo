package nta.catalog.statistics;

import java.util.List;

public class StatisticsUtil {
  public static StatSet aggregate(List<StatSet> statSets) {
    StatSet aggregated = new StatSet();
    
    for (StatSet statSet : statSets) {
      for (Stat stat : statSet.getAllStats()) {
        if (aggregated.containStat(stat.getType())) {
          aggregated.getStat(stat.getType()).incrementBy(stat.getValue());
        } else {
          aggregated.putStat(stat);
        }
      }
    }
    return aggregated;
  }
}
