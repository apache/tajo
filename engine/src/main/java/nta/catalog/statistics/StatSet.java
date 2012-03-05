package nta.catalog.statistics;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import nta.common.ProtoObject;
import nta.engine.TCommonProtos.StatProto;
import nta.engine.TCommonProtos.StatSetProto;
import nta.engine.TCommonProtos.StatSetProtoOrBuilder;
import nta.engine.TCommonProtos.StatType;

import com.google.common.collect.Maps;

/**
 * @author Hyunsik Choi
 */
public class StatSet implements ProtoObject<StatSetProto>, Cloneable {
  private StatSetProto proto = StatSetProto.getDefaultInstance();
  private StatSetProto.Builder builder = null;
  boolean viaProto = false;

  private Map<StatType, Stat> stats;

  public StatSet() {
    builder = StatSetProto.newBuilder();
  }

  public StatSet(StatSetProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  public void addStat(Stat stat) {
    initStats();
    setModified();
    stats.put(stat.getType(), stat);
  }

  public Stat getStat(StatType type) {
    initStats();
    return stats.get(type);
  }

  public Collection<Stat> getAllStats() {
    initStats();
    return stats.values();
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof StatSet) {
      StatSet other = (StatSet) obj;
      for (Entry<StatType, Stat> entry : stats.entrySet()) {
        if (!other.getStat(entry.getKey()).equals(entry.getValue())) {
          return false;
        }
      }      
      return true;
    } else {
      return false;
    }
  }
  
  public Object clone() throws CloneNotSupportedException {
    StatSet group = (StatSet) super.clone();
    initFromProto();
    group.stats = Maps.newHashMap();
    for (Stat stat : stats.values()) {
      group.stats.put(stat.getType(), (Stat) stat.clone());
    }
    
    return group;
  }

  private void initStats() {
    if (this.stats != null) {
      return;
    }
    StatSetProtoOrBuilder p = viaProto ? proto : builder;
    this.stats = Maps.newHashMap();
    for (StatProto statProto : p.getStatsList()) {
      stats.put(statProto.getType(), new Stat(statProto));
    }
  }

  private void setModified() {
    if (viaProto && builder == null) {
      builder = StatSetProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public void initFromProto() {
    StatSetProtoOrBuilder p = viaProto ? proto : builder;
    if (this.stats == null && p.getStatsCount() > 0) {
      this.stats = Maps.newHashMap();
      for (StatProto statProto : p.getStatsList()) {
        this.stats.put(statProto.getType(), new Stat(statProto));
      }
    }
  }

  @Override
  public StatSetProto getProto() {
    if (!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }

    return proto;
  }

  private void mergeLocalToBuilder() {
    if (builder == null) {
      builder = StatSetProto.newBuilder(proto);
    }

    if (this.stats != null) {
      for (Stat stat : stats.values()) {
        builder.addStats(stat.toProto());
      }
    }
  }
}
