package tajo.datum.comparator;

import tajo.datum.FloatDatum;

import java.util.Comparator;

public class FloatComparator implements Comparator<FloatDatum> {

	@Override
	public int compare(FloatDatum o1, FloatDatum o2) {
		return o1.compareTo(o2);
	}

}
