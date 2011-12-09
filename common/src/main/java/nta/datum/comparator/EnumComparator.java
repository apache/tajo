package nta.datum.comparator;

import java.util.Comparator;

import nta.datum.EnumDatum;

public class EnumComparator implements Comparator<EnumDatum> {

	@Override
	public int compare(EnumDatum arg0, EnumDatum arg1) {
		return arg0.compareTo(arg1);
	}

}
