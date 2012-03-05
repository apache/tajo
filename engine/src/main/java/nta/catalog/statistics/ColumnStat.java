/**
 * 
 */
package nta.catalog.statistics;

import nta.catalog.proto.CatalogProtos.ColumnStatProto;
import nta.catalog.proto.CatalogProtos.ColumnStatProtoOrBuilder;
import nta.common.ProtoObject;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * @author Hyunsik Choi
 */
public class ColumnStat implements ProtoObject<ColumnStatProto>, Cloneable {
  private ColumnStatProto proto = ColumnStatProto.getDefaultInstance();
  private ColumnStatProto.Builder builder = null;
  private boolean viaProto = false;

  @Expose
  private Long numDistVals = null;
  @Expose
  private Long numNulls = null;

  public ColumnStat() {
    builder = ColumnStatProto.newBuilder();
  }

  public ColumnStat(ColumnStatProto proto) {
    this.proto = proto;
    this.viaProto = true;
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
      return getNumDistValues().equals(other.getNumDistValues())
          && getNumNulls().equals(other.getNumNulls());
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
    stat.numDistVals = numDistVals;
    stat.numNulls = numNulls;

    return stat;
  }
  
  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(this);
  }

  @Override
  public void initFromProto() {
    ColumnStatProtoOrBuilder p = viaProto ? proto : builder;
    if (this.numDistVals == null && p.hasNumDistVal()) {
      this.numDistVals = p.getNumDistVal();
    }
    if (this.numNulls == null && p.hasNumNulls()) {
      this.numNulls = p.getNumNulls();
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
    if (this.numDistVals != null) {
      builder.setNumDistVal(this.numDistVals);
    }
    if (this.numNulls != null) {
      builder.setNumNulls(this.numNulls);
    }
  }
}
