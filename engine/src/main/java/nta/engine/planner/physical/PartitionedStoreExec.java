/**
 * 
 */
package nta.engine.planner.physical;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

import nta.catalog.Column;
import nta.catalog.Schema;
import nta.catalog.TableMeta;
import nta.catalog.TableMetaImpl;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.planner.logical.StoreTableNode;
import nta.storage.Appender;
import nta.storage.StorageManager;
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
  private final Map<Integer, Appender> appenderMap
    = new HashMap<Integer, Appender>();
  private final Map<Integer, String> prefixMap
    = new HashMap<Integer, String>();
  private Integer incNum = 0;
  
  public PartitionedStoreExec(final StorageManager sm, final int queryId,
      final StoreTableNode annotation, final PhysicalExec subOp) throws IOException {
    Preconditions.checkArgument(annotation.hasPartitionKey());
    
    this.sm = sm;
    this.annotation = annotation;
    this.subOp = subOp;
    this.inputSchema = this.annotation.getInputSchema();
    this.outputSchema = this.annotation.getOutputSchema();    
    this.meta = new TableMetaImpl(this.outputSchema, StoreType.CSV);    
    
    // about the partitions
    this.numPartitions = annotation.getNumPartitions();
    int i = 0;
    this.partitionKeys = new int [annotation.getPartitionKeys().length];
    for (Column key : annotation.getPartitionKeys()) {
      partitionKeys[i] = inputSchema.getColumnId(key.getName());      
      i++;
    }
    this.partitioner = new HashPartitioner(partitionKeys, numPartitions);
    this.storeTable = annotation.getTableName() + "_" + numFormat.format(queryId);
    sm.initTableBase(meta, storeTable);    
  }

  @Override
  public Schema getSchema() {
    return this.outputSchema;
  }
  
  private Appender getAppender(int partition) throws IOException {
    Appender appender = null;
    String prefix = prefixMap.get(partition);
    if (prefix == null) {
      prefix = "p_"+incNum;
      prefixMap.put(partition, prefix);
      appender = sm.getAppender(meta, storeTable,
          prefix);
      appenderMap.put(partition, appender);        
      incNum++;
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