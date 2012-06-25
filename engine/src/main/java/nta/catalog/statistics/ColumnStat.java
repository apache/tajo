/**
 *
 */
package nta.catalog.statistics;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.google.protobuf.ByteString;
import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.ColumnStatProto;
import nta.catalog.proto.CatalogProtos.ColumnStatProtoOrBuilder;
import nta.common.ProtoObject;
import nta.datum.Datum;
import nta.engine.utils.TUtil;
import nta.engine.utils.TupleUtil;

/**
 * @author Hyunsik Choi
 */
public class ColumnStat implements ProtoObject<ColumnStatProto>, Cloneable {
  private ColumnStatProto proto = ColumnStatProto.getDefaultInstance();
  private ColumnStatProto.Builder builder = null;
  private boolean viaProto = false;

  @Expose private Column column = null;
  @Expose private Long numDistVals = null;
  @Expose private Long numNulls = null;
  @Expose private Datum minValue = null;
  @Expose private Datum maxValue = null;

  public ColumnStat(Column column) {
    builder = ColumnStatProto.newBuilder();
    this.column = column;
    numDistVals = 0l;
    numNulls = 0l;
  }

  public ColumnStat(ColumnStatProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  public Column getColumn() {
    ColumnStatProtoOrBuilder p = viaProto ? proto : builder;
    if (column != null) {
      return column;
    }
    if (!p.hasColumn()) {
      return null;
    }
    this.column = new Column(p.getColumn());

    return this.column;
  }

  public Long getNumDistValues() {
    ColumnStatProtoOrBuilder p = viaProto ? proto : builder;
    if (numDistVals != null) {
      return this.numDistVals;
    }
    if (!p.hasNumDistVal()) {
      return null;
    }
    this.numDistVals = p.getNumDistVal();

    return this.numDistVals;
  }

  public void setNumDistVals(long numDistVals) {
    setModified();
    this.numDistVals = numDistVals;
  }

  public boolean minIsNotSet() {
    return minValue == null && (!proto.hasMinValue());
  }

  public Datum getMinValue() {
    ColumnStatProtoOrBuilder p = viaProto ? proto : builder;
    if (minValue != null) {
      return this.minValue;
    }
    if (!p.hasMinValue()) {
      return null;
    }
    this.minValue = TupleUtil.createFromBytes(getColumn().getDataType(), p.getMinValue().toByteArray());

    return this.minValue;
  }

  public void setMinValue(Datum minValue) {
    setModified();
    this.minValue = minValue;
  }

  public boolean maxIsNotSet() {
    return maxValue == null && (!proto.hasMaxValue());
  }

  public Datum getMaxValue() {
    ColumnStatProtoOrBuilder p = viaProto ? proto : builder;
    if (maxValue != null) {
      return this.maxValue;
    }
    if (!p.hasMaxValue()) {
      return null;
    }
    this.maxValue = TupleUtil.createFromBytes(column.getDataType(), p.getMaxValue().toByteArray());

    return this.maxValue;
  }

  public void setMaxValue(Datum maxValue) {
    setModified();
    this.maxValue = maxValue;
  }

  public Long getNumNulls() {
    ColumnStatProtoOrBuilder p = viaProto ? proto : builder;
    if (numNulls != null) {
      return this.numNulls;
    }
    if (!p.hasNumNulls()) {
      return null;
    }
    this.numNulls = p.getNumNulls();

    return this.numNulls;
  }

  public void setNumNulls(long numNulls) {
    setModified();
    this.numNulls = numNulls;
  }

  private void setModified() {
    if (viaProto && builder == null) {
      builder = ColumnStatProto.newBuilder(proto);
    }
    viaProto = false;
  }

  public boolean equals(Object obj) {
    if (obj instanceof ColumnStat) {
      ColumnStat other = (ColumnStat) obj;
      return getColumn().equals(other.getColumn())
          && getNumDistValues().equals(other.getNumDistValues())
          && getNumNulls().equals(other.getNumNulls())
          && TUtil.checkEquals(getMinValue(), other.getMinValue())
          && TUtil.checkEquals(getMaxValue(), other.getMaxValue());
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hashCode(getNumDistValues(), getNumNulls());
  }

  public Object clone() throws CloneNotSupportedException {
    ColumnStat stat = (ColumnStat) super.clone();
    initFromProto();
    stat.column = (Column) this.column.clone();
    stat.numDistVals = numDistVals;
    stat.numNulls = numNulls;
    stat.minValue = minValue;
    stat.maxValue = maxValue;

    return stat;
  }

  public String toString() {
    initFromProto();
    Gson gson = new GsonBuilder().setPrettyPrinting().
        excludeFieldsWithoutExposeAnnotation().create();
    return gson.toJson(this);
  }

  @Override
  public void initFromProto() {
    ColumnStatProtoOrBuilder p = viaProto ? proto : builder;
    if (this.column == null && p.hasColumn()) {
      this.column = new Column(p.getColumn());
    }
    if (this.numDistVals == null && p.hasNumDistVal()) {
      this.numDistVals = p.getNumDistVal();
    }
    if (this.numNulls == null && p.hasNumNulls()) {
      this.numNulls = p.getNumNulls();
    }
    if (this.minValue == null && p.hasMinValue()) {
      this.minValue = TupleUtil.createFromBytes(column.getDataType(), p.getMinValue().toByteArray());
    }
    if (this.maxValue == null && p.hasMaxValue()) {
      this.maxValue = TupleUtil.createFromBytes(column.getDataType(), p.getMaxValue().toByteArray());
    }
  }

  @Override
  public ColumnStatProto getProto() {
    if (!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }

    return proto;
  }

  private void mergeLocalToBuilder() {
    if (builder == null) {
      builder = ColumnStatProto.newBuilder(proto);
    }
    if (this.column != null) {
      builder.setColumn(this.column.getProto());
    }
    if (this.numDistVals != null) {
      builder.setNumDistVal(this.numDistVals);
    }
    if (this.numNulls != null) {
      builder.setNumNulls(this.numNulls);
    }
    if (this.minValue != null) {
      builder.setMinValue(ByteString.copyFrom(this.minValue.asByteArray()));
    }
    if (this.maxValue != null) {
      builder.setMaxValue(ByteString.copyFrom(this.maxValue.asByteArray()));
    }
  }
}
