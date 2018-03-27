package com.hosuke.mapreduce.topn.userLocation;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UserLocationGC extends WritableComparator{

	/**
	 * 指定生成成为什么类的Comparator
	 */
	public UserLocationGC(){
		super(UserLocation.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		UserLocation ul_a = (UserLocation)a;
		UserLocation ul_b = (UserLocation)b;

		int diff_userid = ul_a.getUserid().compareTo(ul_b.getUserid());
		if(diff_userid == 0){
			
			int diff_location = ul_a.getLocationid().compareTo(ul_b.getLocationid());
			if(diff_location == 0){
				
				return 0;
				
			}else{
				return diff_location > 0 ? -1 : 1;
			}
			
		}else{
			return diff_userid > 0 ? -1 : 1;
		}
	}
}
