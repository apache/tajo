package tajo.catalog;

import tajo.catalog.proto.CatalogProtos.*;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.NullProto;
import tajo.rpc.protocolrecords.PrimitiveProtos.StringProto;

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
  TableDescProto getTableDesc(StringProto name);
  
  /**
   * 
   * @return
   * @throws tajo.catalog.exception.CatalogException
   */
  GetAllTableNamesResponse getAllTableNames(NullProto request);
  
  /**
   * 
   * @return
   * @throws tajo.catalog.exception.CatalogException
   */
  GetFunctionsResponse getFunctions(NullProto request);
  
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
  void deleteTable(StringProto name);
  
  BoolProto existsTable(StringProto tableId);
  
  void addIndex(IndexDescProto proto);
  
  BoolProto existIndex(StringProto indexName);
  
  BoolProto existIndex(GetIndexRequest req);
  
  IndexDescProto getIndex(StringProto indexName);
  
  IndexDescProto getIndex(GetIndexRequest req);
  
  void delIndex(StringProto indexName);
  
  /**
   * 
   * @param funcDesc
   */
  void registerFunction(FunctionDescProto funcDesc);
  
  /**
   * 
   * @param signature
   */
  void unregisterFunction(UnregisterFunctionRequest request);
  
  /**
   * 
   * @param signature
   * @return
   */
  FunctionDescProto getFunctionMeta(GetFunctionMetaRequest request);
  
  /**
   * 
   * @param signature
   * @return
   */
  BoolProto containFunction(ContainFunctionRequest request);
}