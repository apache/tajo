/**
 *
 */
package nta.catalog.statistics;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import nta.catalog.Column;
import nta.catalog.proto.CatalogProtos.ColumnStatProto;
import nta.catalog.proto.CatalogProtos.ColumnStatProtoOrBuilder;
import nta.common.ProtoObject;

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
  @Expose private Long minValue = null;
  @Expose private Long maxValue = null;

  public ColumnStat(Column column) {
    builder = ColumnStatProto.newBuilder();
    this.column = column;
    numDistVals = 0l;
    numNulls = 0l;
    minValue = Long.MAX_VALUE;
    maxValue = Long.MIN_VALUE;
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

  public Long getMinValue() {
    ColumnStatProtoOrBuilder p = viaProto ? proto : builder;
    if (minValue != null) {
      return this.minValue;
    }
    if (!p.hasMinValue()) {
      return null;
    }
    this.minValue = p.getMinValue();

    return this.minValue;
  }

  public void setMinValue(long minValue) {
    setModified();
    this.minValue = minValue;
  }

  public Long getMaxValue() {
    ColumnStatProtoOrBuilder p = viaProto ? proto : builder;
    if (maxValue != null) {
      return this.maxValue;
    }
    if (!p.hasMaxValue()) {
      return null;
    }
    this.maxValue = p.getMaxValue();

    return this.maxValue;
  }

  public void setMaxValue(long maxValue) {
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
          && getMinValue().equals(other.getMinValue())
          && getMaxValue().equals(other.getMaxValue());
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
      this.minValue = p.getMinValue();
    }
    if (this.maxValue == null && p.hasMaxValue()) {
      this.maxValue = p.getMaxValue();
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
      builder.setMinValue(this.minValue);
    }
    if (this.maxValue != null) {
      builder.setMaxValue(this.maxValue);
    }
  }
}
