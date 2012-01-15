package nta.catalog;

import java.net.URI;

import org.apache.hadoop.fs.Path;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public interface TableDesc {
  
  void setId(int id);
  
  int getId();
  
  void setName(String name);
  
  String getName();
  
  void setURI(URI uri);
  
  void setURI(Path path);
  
  URI getURI();
  
  void setMeta(TableMeta info);
  
  TableMeta getMeta();
  
  Object clone();
}
