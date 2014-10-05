package org.apache.tajo.engine.planner;

public abstract class AccessPathInfo {
  public enum ScanTypeControl {
    INDEX_SCAN,
    SEQ_SCAN
  }

  private ScanTypeControl scanType;

  public AccessPathInfo(ScanTypeControl scanType) {
    this.scanType = scanType;
  }

  public ScanTypeControl getScanType() {
    return scanType;
  }
}
