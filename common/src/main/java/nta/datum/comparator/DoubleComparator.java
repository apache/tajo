package nta.datum.comparator;

import java.util.Comparator;

import nta.datum.DoubleDatum;

public class DoubleComparator implements Comparator<DoubleDatum> {

	@Override
	public int compare(DoubleDatum o1, DoubleDatum o2) {
		return o1.compareTo(o2);
	}

}
