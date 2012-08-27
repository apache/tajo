package tajo.catalog;

import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.catalog.proto.CatalogProtos.TableProto;
import tajo.catalog.statistics.TableStat;
import tajo.common.ProtoObject;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public interface TableMeta extends ProtoObject<TableProto>, Cloneable {
  
  void setStorageType(StoreType storeType);
  
  StoreType getStoreType();
  
  void setSchema(Schema schema);
  
  Schema getSchema();
  
  void putOption(String key, String val);
  
  void setStat(TableStat stat);
  
  String getOption(String key);
  
  String getOption(String key, String defaultValue);
  
  Iterator<Entry<String,String>> getOptions();
  
  TableStat getStat();
  
  Object clone() throws CloneNotSupportedException;
  
  public String toJSON();
}
