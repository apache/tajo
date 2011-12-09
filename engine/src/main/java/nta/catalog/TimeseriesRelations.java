package nta.catalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import nta.common.type.TimeRange;

public class TimeseriesRelations {

	private String virtualName;
//	private SortedMap<TimeRange, Integer> timeToId;
	private Map<Integer, TimeRange> idToTime;
	private List<Integer> sortedList;
	
	public TimeseriesRelations(String virtualName) {
//		timeToId = new TreeMap<TimeRange, Integer>();
		idToTime = new HashMap<Integer, TimeRange>();
		sortedList = new ArrayList<Integer>();
		this.virtualName = virtualName;
	}
	
	public void addRelation(TimeRange tr, int relId) {
		idToTime.put(relId, tr);
		
		int i = 0;
		for (i = 0; i < sortedList.size(); i++) {
			if (tr.compareTo(idToTime.get(sortedList.get(i))) < 0) {
				sortedList.add(i, relId);
				break;
			}
		}
		if (i == sortedList.size()) {
			sortedList.add(relId);
		}
	}
	
//	public void removeRelation(TimeRange tr, int relId) {
////		timeToId.remove(tr);
//		removeRelation(relId);
//	}
	
//	public void removeRelation(TimeRange tr) {
//		for (Entry<Integer, TimeRange> e: new ArrayList<Entry<Integer, TimeRange>>(idToTime.entrySet())) {
//			if (e.getValue().equals(tr)) {
//				removeRelation(e.getKey());
//			}
//		}
//	}
	
	public void removeRelation(int relId) {
		idToTime.remove(relId);
//		sortedList.remove(relId);
		for (int i = 0; i < sortedList.size(); i++) {
			if (sortedList.get(i) == relId) {
				sortedList.remove(i);
			}
		}
		
	}
	
	public String getVirtualName() {
		return this.virtualName;
	}
	
	public int getRelationId(TimeRange tr) {
		for (Entry<Integer, TimeRange> e: new ArrayList<Entry<Integer, TimeRange>>(idToTime.entrySet())) {
			if (e.getValue().equals(tr)) {
				return (e.getKey());
			}
		}
		return -1;
	}
	
	public TimeRange getTimeRange(int relId) {
		return idToTime.get(relId);
	}
	
	public int getRelIdByTimeOrder(int index) {
		return sortedList.get(index);
	}
	
	public int size() {
		return sortedList.size();
	}
	
//	public int getOldestRelationId() {
//		return sortedList.get(0);
//	}
	
//	public List<TimeRange> getKeyList() {
//		return Collections.synchronizedList(new ArrayList<TimeRange>(timeToId.keySet()));
//	}
	
}
