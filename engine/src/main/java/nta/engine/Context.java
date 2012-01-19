package nta.engine;

import nta.catalog.TableDesc;

/**
 * 이 클래스는 주어진 질의가 실행 중인 동안 질의에 대한 정보를 유지한다.
 * 
 * @author Hyunsik Choi
 */
public interface Context {
  TableDesc getInputTable(String id);

  CatalogReader getCatalog();
}