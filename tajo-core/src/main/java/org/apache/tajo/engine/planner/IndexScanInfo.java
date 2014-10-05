package org.apache.tajo.engine.planner;

import org.apache.tajo.catalog.IndexDesc;
import org.apache.tajo.datum.Datum;

public class IndexScanInfo extends AccessPathInfo {
  private IndexDesc indexDesc;
  private Datum[] values;

  public IndexScanInfo() {
    super(ScanTypeControl.INDEX_SCAN);
  }

  public IndexScanInfo(IndexDesc indexDesc, Datum[] values) {
    this();
    this.setIndexDesc(indexDesc);
  }

  public void setIndexDesc(IndexDesc indexDesc) {
    this.indexDesc = indexDesc;
  }
  public void setValues(Datum[] values) {
    this.values = values;
  }

  public IndexDesc getIndexDesc() {
    return indexDesc;
  }
}
