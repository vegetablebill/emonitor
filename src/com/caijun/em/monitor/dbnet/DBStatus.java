package com.caijun.em.monitor.dbnet;

import java.util.Date;

public class DBStatus {
	private long id;
	private long dbid;
	private int disconn;
	private Date ct;
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public long getDbid() {
		return dbid;
	}
	public void setDbid(long dbid) {
		this.dbid = dbid;
	}
	public boolean isDisconn() {
		if(disconn==1){
			return true;
		}else{
			return false;
		}
	}
	public void setDisconn(boolean disconn) {
		if(disconn){
			this.disconn=1;
		}else{
			this.disconn = 0;
		}
	}
	public Date getCt() {
		return ct;
	}
	public void setCt(Date ct) {
		this.ct = ct;
	}
	
}
