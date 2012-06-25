package nta.catalog.statistics;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * @author Hyunsik Choi
 */
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

  public static TableStat aggregate(List<TableStat> tableStats) {
    TableStat aggregated = new TableStat();

    ColumnStat [] css = null;
    if (tableStats.size() > 0 && tableStats.get(0).getColumnStats().size() > 0) {
      css = new ColumnStat[tableStats.get(0).getColumnStats().size()];
      for (int i = 0; i < css.length; i++) {
        css[i] = new ColumnStat(tableStats.get(0).getColumnStats().get(i).getColumn());
      }
    }

    for (TableStat ts : tableStats) {
      // aggregate column stats for each table
      for (int i = 0; i < ts.getColumnStats().size(); i++) {
        css[i].setNumDistVals(css[i].getNumDistValues() + ts.getColumnStats().get(i).getNumDistValues());
        css[i].setNumNulls(css[i].getNumNulls() + ts.getColumnStats().get(i).getNumNulls());
        css[i].setMinValue(Math.min(css[i].getMinValue(), ts.getColumnStats().get(i).getMinValue()));
        css[i].setMaxValue(Math.max(css[i].getMaxValue(), ts.getColumnStats().get(i).getMaxValue()));
      }

      // aggregate table stats for each table
      aggregated.setNumRows(aggregated.getNumRows() + ts.getNumRows());
      aggregated.setNumBytes(aggregated.getNumBytes() + ts.getNumBytes());
      aggregated.setNumBlocks(aggregated.getNumBlocks() + ts.getNumBlocks());
      aggregated.setNumPartitions(aggregated.getNumPartitions() + ts.getNumPartitions());
    }

    //aggregated.setAvgRows(aggregated.getNumRows() / tableStats.size());
    if (css != null) {
      aggregated.setColumnStats(Lists.newArrayList(css));
    }

    return aggregated;
  }
}