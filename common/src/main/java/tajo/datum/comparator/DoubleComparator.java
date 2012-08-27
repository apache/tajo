package tajo.datum.comparator;

import tajo.datum.DoubleDatum;

import java.util.Comparator;

public class DoubleComparator implements Comparator<DoubleDatum> {

	@Override
	public int compare(DoubleDatum o1, DoubleDatum o2) {
		return o1.compareTo(o2);
	}

}
