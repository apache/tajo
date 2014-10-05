package org.apache.tajo.engine.planner;

import org.apache.tajo.catalog.TableDesc;

public class SeqScanInfo extends AccessPathInfo {
  private TableDesc tableDesc;

  public SeqScanInfo() {
    super(ScanTypeControl.SEQ_SCAN);
  }

  public SeqScanInfo(TableDesc tableDesc) {
    this();
    this.setTableDesc(tableDesc);
  }

  public TableDesc getTableDesc() {
    return tableDesc;
  }

  public void setTableDesc(TableDesc tableDesc) {
    this.tableDesc = tableDesc;
  }
}
