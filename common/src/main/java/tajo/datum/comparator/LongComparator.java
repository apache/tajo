package tajo.datum.comparator;

import tajo.datum.LongDatum;

import java.util.Comparator;


public class LongComparator implements Comparator<LongDatum> {

	@Override
	public int compare(LongDatum o1, LongDatum o2) {
		return o1.compareTo(o2);
	}

}
