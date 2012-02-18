/**
 * 
 */
package nta.engine.planner.physical;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.QueryUnitId;
import nta.engine.planner.logical.StoreTableNode;
import nta.storage.Appender;
import nta.storage.StorageManager;
import nta.storage.StorageUtil;
import nta.storage.Tuple;

import com.google.common.base.Preconditions;

/**
 * @author Hyunsik Choi
 */
public final class PartitionedStoreExec extends PhysicalExec {
  private static final NumberFormat numFormat = NumberFormat.getInstance();

  static {
    numFormat.setGroupingUsed(false);
    numFormat.setMinimumIntegerDigits(6);
  }
  
  private final StorageManager sm;
  private final StoreTableNode annotation;
  private final PhysicalExec subOp;
  
  private final Schema inputSchema;
  private final Schema outputSchema;
  private final int numPartitions;
  private final int [] partitionKeys;  
  
  private final TableMeta meta;
  private final Partitioner partitioner;
  private final String storeTable;
  private final Path storeTablePath;
  private final Map<Integer, Appender> appenderMap
    = new HashMap<Integer, Appender>();
  
  public PartitionedStoreExec(final StorageManager sm, final QueryUnitId queryId,
      final StoreTableNode annotation, final PhysicalExec subOp) throws IOException {
    Preconditions.checkArgument(annotation.hasPartitionKey());
    
    this.sm = sm;
    this.annotation = annotation;
    this.subOp = subOp;
    this.inputSchema = this.annotation.getInputSchema();
    this.outputSchema = this.annotation.getOutputSchema();    
    this.meta = TCatUtil.newTableMeta(this.outputSchema, StoreType.CSV);    
    
    // about the partitions
    this.numPartitions = annotation.getNumPartitions();
    int i = 0;
    this.partitionKeys = new int [annotation.getPartitionKeys().length];
    for (Column key : annotation.getPartitionKeys()) {
      partitionKeys[i] = inputSchema.getColumnId(key.getQualifiedName());      
      i++;
    }
    this.partitioner = new HashPartitioner(partitionKeys, numPartitions);
    this.storeTable = queryId.toString();
    
    storeTablePath = StorageUtil.concatPath( 
        sm.getTablePath(annotation.getTableName()),
            queryId.toString());
    sm.initTableBase(storeTablePath, meta);
    Log.info("Initialized output directory (" 
        + sm.getTablePath(storeTable) + ")");
  }

  @Override
  public Schema getSchema() {
    return this.outputSchema;
  }
  
  private Appender getAppender(int partition) throws IOException {
    Appender appender = appenderMap.get(partition);
    if (appender == null) {
      Path dataFile =
          StorageUtil.concatPath(storeTablePath, "data",
              "" + partition);
      appender = sm.getAppender(meta, dataFile);
      appenderMap.put(partition, appender);
    } else {
      appender = appenderMap.get(partition);
    }
    
    return appender;
  }

  @Override
  public Tuple next() throws IOException {
    Tuple tuple = null;
    Appender appender = null;
    int partition;
    while ((tuple = subOp.next()) != null) {
      partition = partitioner.getPartition(tuple);
      appender = getAppender(partition);
      appender.addTuple(tuple);
    }
    
    for (Appender app : appenderMap.values()) {
      app.flush();
      app.close();
    }
    
    return null;
  }
}