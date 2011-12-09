package nta.datum.comparator;

import java.util.Comparator;

import nta.datum.StringDatum;

public class CharComparator implements Comparator<StringDatum> {

	@Override
	public int compare(StringDatum o1, StringDatum o2) {
		return o1.compareTo(o2);
	}

}
