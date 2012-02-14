package nta.catalog;

import nta.catalog.proto.CatalogProtos.ContainFunctionRequest;
import nta.catalog.proto.CatalogProtos.FunctionDescProto;
import nta.catalog.proto.CatalogProtos.GetAllTableNamesResponse;
import nta.catalog.proto.CatalogProtos.GetFunctionMetaRequest;
import nta.catalog.proto.CatalogProtos.GetFunctionsResponse;
import nta.catalog.proto.CatalogProtos.TableDescProto;
import nta.catalog.proto.CatalogProtos.UnregisterFunctionRequest;
import nta.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import nta.rpc.protocolrecords.PrimitiveProtos.NullProto;
import nta.rpc.protocolrecords.PrimitiveProtos.StringProto;

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
   * @throws CatalogException
   */
  GetAllTableNamesResponse getAllTableNames(NullProto request);
  
  /**
   * 
   * @return
   * @throws CatalogException
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