package nta.catalog;

import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.proto.CatalogProtos.TableProto;
import nta.common.ProtoObject;

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
  
  String getOption(String key, String defaultValue);
  
  Object clone() throws CloneNotSupportedException;
  
  public String toJSON();
}
