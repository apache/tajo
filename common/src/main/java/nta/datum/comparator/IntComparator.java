package nta.datum.comparator;

import java.util.Comparator;

import nta.datum.IntDatum;

public class IntComparator implements Comparator<IntDatum> {

	@Override
	public int compare(IntDatum o1, IntDatum o2) {
		return o1.compareTo(o2);
	}

}
