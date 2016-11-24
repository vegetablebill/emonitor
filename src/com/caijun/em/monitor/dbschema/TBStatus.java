package com.caijun.em.monitor.dbschema;

import java.util.Date;

import com.tongtech.cj.dbMeta.TBMeta;

public class TBStatus {
	private long id;
	private long tbid;
	private TBMeta meta;
	private int droped;
	private Date ct;

	public long getId() {
		return id;
	}

	void setId(long id) {
		this.id = id;
	}

	public long getTbid() {
		return tbid;
	}

	void setTbid(long tbid) {
		this.tbid = tbid;
	}

	public TBMeta getStrcut() {
		return meta;
	}

	void setStrcut(TBMeta meta) {
		this.meta = meta;
	}

	public boolean isDroped() {
		if (droped == 1) {
			return true;
		} else {
			return false;
		}
	}

	void setDroped(boolean droped) {
		if (droped) {
			this.droped = 1;
		} else {
			this.droped = 0;
		}
	}

	public Date getCt() {
		return ct;
	}

	void setCt(Date ct) {
		this.ct = ct;
	}

}
