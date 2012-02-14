package nta.catalog;

import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.proto.CatalogProtos.TableDescProto;

import org.apache.hadoop.fs.Path;

import com.google.protobuf.Message;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public interface TableDesc extends Cloneable {
  void setId(String tableId);
  
  String getId();
  
  void setPath(Path path);
  
  Path getPath();
  
  void setMeta(TableMeta info);
  
  TableMeta getMeta();
  
  Message getProto();
  
  public void initFromProto();
  
  public String toJSON();
  
  public Object clone() throws CloneNotSupportedException;
  
  public static class Factory {
    public static TableDesc create(String tableId, Schema schema, 
        StoreType type) {
      return new TableDescImpl(tableId, new TableMetaImpl(schema, type));   
    }
    
    public static TableDesc create(TableDescProto proto) {
      return new TableDescImpl(proto);
    }
  }
}