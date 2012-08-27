package tajo.catalog.store;

import tajo.catalog.FunctionDesc;
import tajo.catalog.TableDesc;
import tajo.catalog.proto.CatalogProtos.IndexDescProto;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/** 
 * @author Hyunsik Choi
 */
public interface CatalogStore extends Closeable { 
  void addTable(TableDesc desc) throws IOException;
  
  boolean existTable(String name) throws IOException;
  
  void deleteTable(String name) throws IOException;
  
  TableDesc getTable(String name) throws IOException;
  
  List<String> getAllTableNames() throws IOException;
  
  void addIndex(IndexDescProto proto) throws IOException;
  
  void delIndex(String indexName) throws IOException;
  
  IndexDescProto getIndex(String indexName) throws IOException;
  
  IndexDescProto getIndex(String tableName, String columnName) 
      throws IOException;
  
  boolean existIndex(String indexName) throws IOException;
  
  boolean existIndex(String tableName, String columnName) throws IOException;
  
  IndexDescProto [] getIndexes(String tableName) throws IOException;
  
  void addFunction(FunctionDesc func) throws IOException;
  
  void deleteFunction(FunctionDesc func) throws IOException;
  
  void existFunction(FunctionDesc func) throws IOException;
  
  List<String> getAllFunctionNames() throws IOException;
}
