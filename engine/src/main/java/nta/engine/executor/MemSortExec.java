package nta.engine.executor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import nta.catalog.Column;
import nta.datum.comparator.ByteComparator;
import nta.datum.comparator.CharComparator;
import nta.datum.comparator.DoubleComparator;
import nta.datum.comparator.EnumComparator;
import nta.datum.comparator.FloatComparator;
import nta.datum.comparator.IPv4Comparator;
import nta.datum.comparator.IPv6Comparator;
import nta.datum.comparator.IntComparator;
import nta.datum.comparator.LongComparator;
import nta.datum.comparator.ShortComparator;
import nta.storage.Tuple;

public class MemSortExec extends SortExec {

	public MemSortExec(ScanExec scan, Column[] cols, boolean desc)
			throws Exception {
		super(scan, cols, desc);
		List<Tuple> l = new ArrayList<Tuple>();
		Tuple next = null;
		while ((next = scan.next()) != null) {
			l.add(next);
		}
		sortedScan = new Tuple[l.size()];
		sortedScan = l.toArray(sortedScan);
		
		Comparator comp = null;
		for (int i = 0; i < cols.length; i++) {
			switch (cols[i].getDataType()) {
			case BYTE:
				comp = new ByteComparator();
				break;
			case SHORT:
				comp = new ShortComparator();
				break;
			case INT:
				comp = new IntComparator();
				break;
			case LONG:
				comp = new LongComparator();
				break;
			case FLOAT:
				comp = new FloatComparator();
				break;
			case DOUBLE:
				comp = new DoubleComparator();
				break;
			case STRING:
				comp = new CharComparator();
				break;
			case ENUM:
				comp = new EnumComparator();
				break;
			case IPv4:
				comp = new IPv4Comparator();
				break;
			case IPv6:
				comp = new IPv6Comparator();
				break;
			default:
				break;
			}
		}
		Arrays.sort(sortedScan, comp);
		currentRecord = 0;
	}

}
