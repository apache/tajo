package nta.cube;

import java.util.LinkedList;

/* SummaryTable structure */
public class SummaryTable {
  LinkedList<KVpair> summary_table;
  int count;

  public SummaryTable() {
    summary_table = new LinkedList<KVpair>();
    count = 0;
  }
}
