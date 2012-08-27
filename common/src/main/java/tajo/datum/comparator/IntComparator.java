package tajo.datum.comparator;

import tajo.datum.IntDatum;

import java.util.Comparator;

public class IntComparator implements Comparator<IntDatum> {

	@Override
	public int compare(IntDatum o1, IntDatum o2) {
		return o1.compareTo(o2);
	}

}
