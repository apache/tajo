package nta.catalog;

import org.apache.hadoop.fs.Path;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public interface TableDesc {  
  void setId(String tableId);
  
  String getId();
  
  void setPath(Path path);
  
  Path getPath();
  
  void setMeta(TableMeta info);
  
  TableMeta getMeta();
  
  Object clone();
}