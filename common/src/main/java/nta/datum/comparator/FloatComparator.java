package nta.datum.comparator;

import java.util.Comparator;

import nta.datum.FloatDatum;

public class FloatComparator implements Comparator<FloatDatum> {

	@Override
	public int compare(FloatDatum o1, FloatDatum o2) {
		return o1.compareTo(o2);
	}

}
