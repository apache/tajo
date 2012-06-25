package nta.catalog.statistics;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos;
import nta.datum.Datum;
import nta.datum.DatumType;
import nta.storage.Tuple;
import nta.storage.VTuple;

/**
 * This class is not thread-safe.
 *
 * @author Hyunsik Choi
 */
public class TableStatistics {
  private Schema schema;
  private Tuple minValues;
  private Tuple maxValues;
  private long [] numNulls;
  private long numRows = 0;
  private long numBytes = 0;


  private boolean [] comparable;

  public TableStatistics(Schema schema) {
    this.schema = schema;
    minValues = new VTuple(schema.getColumnNum());
    maxValues = new VTuple(schema.getColumnNum());
    /*for (int i = 0; i < schema.getColumnNum(); i++) {
      minValues[i] = Long.MAX_VALUE;
      maxValues[i] = Long.MIN_VALUE;
    }*/

    numNulls = new long[schema.getColumnNum()];
    comparable = new boolean[schema.getColumnNum()];

    CatalogProtos.DataType type;
    for (int i = 0; i < schema.getColumnNum(); i++) {
      type = schema.getColumn(i).getDataType();
      if (type == CatalogProtos.DataType.ARRAY) {
        comparable[i] = false;
      } else {
        comparable[i] = true;
      }
    }
  }

  public Schema getSchema() {
    return this.schema;
  }

  public void incrementRow() {
    numRows++;
  }

  public long getNumRows() {
    return this.numRows;
  }

  public void setNumBytes(long bytes) {
    this.numBytes = bytes;
  }

  public long getNumBytes() {
    return this.numBytes;
  }

  public void analyzeField(int idx, Datum datum) {
    if (datum.type() == DatumType.NULL) {
      numNulls[idx]++;
      return;
    }

    if (datum.type() != DatumType.ARRAY) {
      if (comparable[idx]) {
        if (!maxValues.contains(idx) ||
            maxValues.get(idx).compareTo(datum) < 0) {
          maxValues.put(idx, datum);
        }
        if (!minValues.contains(idx) ||
            minValues.get(idx).compareTo(datum) > 0) {
          minValues.put(idx, datum);
        }
      }
    }
  }

  public TableStat getTableStat() {
    TableStat stat = new TableStat();

    ColumnStat columnStat;
    for (int i = 0; i < schema.getColumnNum(); i++) {
      columnStat = new ColumnStat(schema.getColumn(i));
      columnStat.setNumNulls(numNulls[i]);
      columnStat.setMinValue(minValues.get(i));
      columnStat.setMaxValue(maxValues.get(i));
      stat.addColumnStat(columnStat);
    }

    stat.setNumRows(this.numRows);
    stat.setNumBytes(this.numBytes);

    return stat;
  }
}
