package nta.catalog;

import java.util.Collection;

import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.FunctionDescProto;
import nta.catalog.proto.CatalogProtos.TableDescProto;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public interface CatalogServiceProtocol {
  
  /**
   * Get a table description by name
   * @param name table name
   * @return a table description
   * @see TableDescImpl
   * @throws Throwable
   */
  TableDescProto getTableDesc(String name);
  
  /**
   * 
   * @return
   * @throws CatalogException
   */
  Collection<String> getAllTableNames();
  
  /**
   * 
   * @return
   * @throws CatalogException
   */
  Collection<FunctionDescProto> getFunctions();
  
  /**
   * Add a table via table description
   * @param meta table meta
   * @see TableDescImpl
   * @throws Throwable
   */
  void addTable(TableDescProto desc);
  
  /**
   * Drop a table by name
   * @param name table name
   * @throws Throwable
   */
  void deleteTable(String name);
  
  boolean existsTable(String tableId);
  
  /**
   * 
   * @param funcDesc
   */
  void registerFunction(FunctionDescProto funcDesc);
  
  /**
   * 
   * @param signature
   */
  void unregisterFunction(String signature, DataType...paramTypes);
  
  /**
   * 
   * @param signature
   * @return
   */
  FunctionDescProto getFunctionMeta(String signature, DataType...paramTypes);
  
  /**
   * 
   * @param signature
   * @return
   */
  boolean containFunction(String signature, DataType...paramTypes);
}