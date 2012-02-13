package nta.catalog.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import nta.catalog.FunctionDesc;
import nta.catalog.TableDesc;

/** 
 * @author Hyunsik Choi
 */
public interface CatalogStore extends Closeable { 
  void addTable(TableDesc desc) throws IOException;
  
  boolean existTable(String name) throws IOException;
  
  void deleteTable(String name) throws IOException;
  
  TableDesc getTable(String name) throws IOException;
  
  List<String> getAllTableNames() throws IOException;
  
  void addFunction(FunctionDesc func) throws IOException;
  
  void deleteFunction(FunctionDesc func) throws IOException;
  
  void existFunction(FunctionDesc func) throws IOException;
  
  List<String> getAllFunctionNames() throws IOException;
}
