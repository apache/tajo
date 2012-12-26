package tajo.storage.hcfile;

import org.apache.hadoop.io.Writable;
import tajo.catalog.proto.CatalogProtos.CompressType;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HColumnMetaWritable implements ColumnMeta, Writable {
  public static final int SIZE = Long.SIZE/8
      + 2 * Integer.SIZE/8
      + 2 * Short.SIZE/8
      + 3 * Byte.SIZE/8;


  private long startRid;
  private int recordNum;
  private int offsetToIndex;
  private DataType dataType;
  private CompressType compType;
  private boolean compressed;
  private boolean sorted;
  private boolean contiguous;

  public HColumnMetaWritable() {

  }

  public HColumnMetaWritable(long startRid, DataType dataType,
                             CompressType compType, boolean compressed,
                             boolean sorted, boolean contiguous) {
    this.startRid = startRid;
    this.dataType = dataType;
    this.compType = compType;
    this.compressed = compressed;
    this.sorted = sorted;
    this.contiguous = contiguous;
  }

  public void setStartRid(long startRid) {
    this.startRid = startRid;
  }

  public void setRecordNum(int recordNum) {
    this.recordNum = recordNum;
  }

  public void setOffsetToIndex(int offsetToIndex) {
    this.offsetToIndex = offsetToIndex;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  public void setCompType(CompressType compType) {
    this.compType = compType;
  }

  public void setCompressed(boolean compressed) {
    this.compressed = compressed;
  }

  public void setSorted(boolean sorted) {
    this.sorted = sorted;
  }

  public void setContiguous(boolean contiguous) {
    this.contiguous = contiguous;
  }

  @Override
  public StoreType getStoreType() {
    return StoreType.HCFILE;
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  @Override
  public CompressType getCompressType() {
    return compType;
  }

  @Override
  public boolean isCompressed() {
    return compressed;
  }

  @Override
  public boolean isSorted() {
    return sorted;
  }

  @Override
  public boolean isContiguous() {
    return contiguous;
  }

  public long getStartRid() {
    return startRid;
  }

  public int getRecordNum() {
    return recordNum;
  }

  public long getOffsetToIndex() {
    return this.offsetToIndex;

  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(startRid);
    out.writeInt(recordNum);
    out.writeInt(offsetToIndex);
    out.writeShort(dataType.getNumber());
    out.writeShort(compType.getNumber());
    out.writeBoolean(compressed);
    out.writeBoolean(sorted);
    out.writeBoolean(contiguous);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    startRid = in.readLong();
    recordNum = in.readInt();
    offsetToIndex = in.readInt();
    dataType = DataType.valueOf(in.readShort());
    compType = CompressType.valueOf(in.readShort());
    compressed = in.readBoolean();
    sorted = in.readBoolean();
    contiguous = in.readBoolean();
  }

  public static int size() {
    return SIZE;
  }

}
