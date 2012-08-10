/**
 * 
 */
package nta.engine.query;

import nta.catalog.statistics.Stat;
import nta.catalog.statistics.StatSet;
import nta.catalog.statistics.TableStat;
import nta.common.exception.NotImplementedException;
import nta.engine.MasterInterfaceProtos.InProgressStatusProto;
import nta.engine.QueryIdFactory;
import nta.engine.planner.global.QueryUnit;
import nta.engine.planner.logical.LogicalNode;

/**
 * @author jihoon
 *
 */
public class TQueryUtil {

  public static TableStat mergeStatSet(TableStat tableStat, StatSet statSet) {
    for (Stat stat : statSet.getAllStats()) {
      switch (stat.getType()) {
      case COLUMN_NUM_NULLS:
        // TODO
        throw new NotImplementedException();
      case TABLE_AVG_ROWS:
        if (tableStat.getAvgRows() == null) {
          tableStat.setAvgRows(stat.getValue());
        } else {
          tableStat.setAvgRows(tableStat.getAvgRows()+stat.getValue());
        }
        break;
      case TABLE_NUM_BLOCKS:
        if (tableStat.getNumBlocks() == null) {
          tableStat.setNumBlocks((int)stat.getValue());
        } else {
          tableStat.setNumBlocks(tableStat.getNumBlocks()+
              (int)stat.getValue());
        }
        break;
      case TABLE_NUM_BYTES:
        if (tableStat.getNumBytes() == null) {
          tableStat.setNumBytes(stat.getValue());
        } else {
          tableStat.setNumBytes(tableStat.getNumBytes()+stat.getValue());
        }
        break;
      case TABLE_NUM_PARTITIONS:
        if (tableStat.getNumPartitions() == null) {
          tableStat.setNumPartitions((int)stat.getValue());
        } else {
          tableStat.setNumPartitions(tableStat.getNumPartitions()+
              (int)stat.getValue());
        }
        break;
      case TABLE_NUM_ROWS:
        if (tableStat.getNumRows() == null) {
          tableStat.setNumRows(stat.getValue());
        } else {
          tableStat.setNumRows(tableStat.getNumRows()+stat.getValue());
        }
        break;
      }
    }
    
    return tableStat;
  }
  
  public static InProgressStatusProto getInProgressStatusProto(QueryUnit unit) {
    InProgressStatusProto.Builder builder = InProgressStatusProto.newBuilder();
    builder.setId(unit.getLastAttempt().getId().getProto());
    builder.setStatus(unit.getStatus());
    builder.setProgress(unit.getProgress());
    builder.addAllPartitions(unit.getPartitions());
    builder.setResultStats(unit.getStats().getProto());
    return builder.build();
  }
}
