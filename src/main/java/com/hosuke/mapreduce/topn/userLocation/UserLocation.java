package com.hosuke.mapreduce.topn.userLocation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserLocation implements WritableComparable<UserLocation> {

	private String userid;
	private String locationid;
	private String time;
	private long duration;

	@Override
	public String toString() {
		return userid + "\t" + locationid + "\t" + time + "\t" + duration;
	}

	public UserLocation() {
		super();
	}
	
	public void set(String[] split){
		this.setUserid(split[0]);
		this.setLocationid(split[1]);
		this.setTime(split[2]);
		this.setDuration(Long.parseLong(split[3]));
	}
	
	public void set(UserLocation ul){
		this.setUserid(ul.getUserid());
		this.setLocationid(ul.getLocationid());
		this.setTime(ul.getTime());
		this.setDuration(ul.getDuration());
	}

	public UserLocation(String userid, String locationid, String time, long duration) {
		super();
		this.userid = userid;
		this.locationid = locationid;
		this.time = time;
		this.duration = duration;
	}

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public String getLocationid() {
		return locationid;
	}

	public void setLocationid(String locationid) {
		this.locationid = locationid;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public long getDuration() {
		return duration;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(userid);
		out.writeUTF(locationid);
		out.writeUTF(time);
		out.writeLong(duration);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.userid = in.readUTF();
		this.locationid = in.readUTF();
		this.time = in.readUTF();
		this.duration = in.readLong();
	}

	/**
	 * 排序规则
	 * 
	 * 按照 userid  locationid  和  time 排序  都是 升序
	 */
	@Override
	public int compareTo(UserLocation o) {

		int diff_userid = o.getUserid().compareTo(this.getUserid());
		if(diff_userid == 0){
			
			int diff_location = o.getLocationid().compareTo(this.getLocationid());
			if(diff_location == 0){
				
				int diff_time = o.getTime().compareTo(this.getTime());
				if(diff_time == 0){
					return 0;
				}else{
					return diff_time > 0 ? -1 : 1;
				}
				
			}else{
				return diff_location > 0 ? -1 : 1;
			}
			
		}else{
			return diff_userid > 0 ? -1 : 1;
		}
	}
}
