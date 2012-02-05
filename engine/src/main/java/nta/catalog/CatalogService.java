package nta.catalog;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import nta.catalog.exception.CatalogException;
import nta.catalog.proto.CatalogProtos.DataType;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public interface CatalogService {
  
  /**
   * Get a table description by name
   * @param name table name
   * @return a table description
   * @see TableDescImpl
   * @throws Throwable
   */
  TableDesc getTableDesc(String name);
  
  /**
   * 
   * @return
   * @throws CatalogException
   */
  Collection<TableDesc> getAllTableDescs();
  
  /**
   * 
   * @return
   * @throws CatalogException
   */
  Collection<FunctionDesc> getFunctions();
  
  /**
   * Add a table via table description
   * @param meta table meta
   * @see TableDescImpl
   * @throws Throwable
   */
  void addTable(TableDesc desc);
  
  /**
   * Drop a table by name
   * @param name table name
   * @throws Throwable
   */
  void deleteTable(String name);
  
  boolean existsTable(String tableId);
  
  void registerFunction(FunctionDesc funcDesc);
 
  void unregisterFunction(String signature, DataType...paramTypes);
  
  /**
   * 
   * @param signature
   * @return
   */
  FunctionDesc getFunction(String signature, DataType...paramTypes);
  
  /**
   * 
   * @param signature
   * @return
   */
  boolean containFunction(String signature, DataType...paramTypes);
  
  List<HostInfo> getHostByTable(String tableId);
  
  void updateAllTabletServingInfo(List<String> onlineServers) throws IOException;
}