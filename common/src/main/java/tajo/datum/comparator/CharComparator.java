package tajo.datum.comparator;

import tajo.datum.StringDatum;

import java.util.Comparator;

public class CharComparator implements Comparator<StringDatum> {

	@Override
	public int compare(StringDatum o1, StringDatum o2) {
		return o1.compareTo(o2);
	}

}
