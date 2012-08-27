package tajo.catalog;

import com.google.protobuf.Message;
import org.apache.hadoop.fs.Path;

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
  
  void initFromProto();
  
  String toJSON();
 
  Object clone() throws CloneNotSupportedException;
}