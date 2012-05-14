package nta.engine.planner.physical;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import nta.catalog.Schema;
import nta.catalog.TCatUtil;
import nta.catalog.TableMeta;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.engine.planner.logical.SortNode;
import nta.storage.Appender;
import nta.storage.Scanner;
import nta.storage.StorageManager;
import nta.storage.Tuple;

/**
 * @author Byungnam Lim
 */
public class ExternalSortExec extends PhysicalExec {
  private PhysicalExec subOp;
  private final Schema inputSchema;
  private final Schema outputSchema;

  private final Comparator<Tuple> comparator;
  private final List<Tuple> tupleSlots;
  private boolean sorted = false;
  private StorageManager sm;
  private Scanner s;
  private Appender appender;
  private String tableName = null;

  private final int MAXSIZE = 10000;
  private int run;
  private final static String SORT_PREFIX = "s_";

  public ExternalSortExec(StorageManager sm, SortNode annotation,
      PhysicalExec subOp) {
    this.subOp = subOp;
    this.sm = sm;

    this.inputSchema = annotation.getInputSchema();
    this.outputSchema = annotation.getOutputSchema();

    this.comparator = new TupleComparator(inputSchema,
        annotation.getSortKeys(), null);
    this.tupleSlots = new ArrayList<Tuple>(MAXSIZE);

    this.run = 0;
  }

  @Override
  public Schema getSchema() {
    return this.outputSchema;
  }

  private void firstPhase(List<Tuple> tupleSlots) throws IOException {
    TableMeta meta = TCatUtil.newTableMeta(this.inputSchema, StoreType.CSV);
    Collections.sort(tupleSlots, this.comparator);
    sm.initTableBase(meta, SORT_PREFIX + "0_" + run);
    appender = sm.getAppender(meta, SORT_PREFIX+"0_" + run, SORT_PREFIX + "0_" + run);
    for (Tuple t : tupleSlots) {
      appender.addTuple(t);
    }
    appender.flush();
    appender.close();
    tupleSlots.clear();
    run++;
  }

  @Override
  public Tuple next() throws IOException {
    if (sorted == false) {
      Tuple tuple = null;
      int runNum = 0;
      while ((tuple = subOp.next()) != null) { // partition sort start
        tupleSlots.add(tuple);
        if (tupleSlots.size() == MAXSIZE) {
          firstPhase(tupleSlots);
        }
      }

      firstPhase(tupleSlots);
      runNum = run;

      int iterator = 0;
      run = 0;

      TableMeta meta;
      // external sort start
      while (runNum > 1) {
        while (run < runNum) {
          meta = TCatUtil.newTableMeta(this.inputSchema, StoreType.CSV);
          sm.initTableBase(meta, SORT_PREFIX + (iterator + 1) + "_" + (run / 2));
          appender = sm.getAppender(meta, SORT_PREFIX + (iterator + 1) + "_"
              + (run / 2), SORT_PREFIX + (iterator + 1) + "_" + (run / 2));

          if (run + 1 >= runNum) { // if number of run is odd just copy it.
            Scanner s1 = sm.getScanner(SORT_PREFIX + iterator + "_" + run, SORT_PREFIX
                + iterator + "_" + run);
            while ((tuple = s1.next()) != null) {
              appender.addTuple(tuple);
            }
          } else {
            Scanner s1 = sm.getScanner(SORT_PREFIX + iterator + "_" + run, SORT_PREFIX
                + iterator + "_" + run);
            Scanner s2 = sm.getScanner(SORT_PREFIX + iterator + "_" + (run + 1),
                SORT_PREFIX + iterator + "_" + (run + 1));

            Tuple left = s1.next();
            Tuple right = s2.next();

            while (left != null && right != null) {
              if (this.comparator.compare(left, right) < 0) {
                appender.addTuple(left);
                left = s1.next();
              } else {
                appender.addTuple(right);
                right = s2.next();
              }
            }

            if (left == null) {
              appender.addTuple(right);
              while ((right = s2.next()) != null) {
                appender.addTuple(right);
              }
            } else {
              appender.addTuple(left);
              while ((left = s1.next()) != null) {
                appender.addTuple(left);
              }
            }
          }
          appender.flush();
          appender.close();
          run += 2;
        }
        iterator++;
        run = 0;
        runNum = runNum / 2 + runNum % 2;
      }
      tableName = new String(SORT_PREFIX + iterator + "_" + 0);
      s = sm.getScanner(tableName, tableName);
      sorted = true;
    }

    return s.next();
  }

  @Override
  public void rescan() throws IOException {
    if (s != null) {
      s.reset();
    }
  }
}
