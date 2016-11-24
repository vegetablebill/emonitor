package com.caijun.em.monitor.dbschema;

import java.util.Date;

import com.tongtech.cj.dbMeta.TrigMeta;

public class TrigStatus {
	private long id;
	private long trigid;
	private TrigMeta meta;
	private int droped;
	private int disabled;
	private Date ct;

	public long getId() {
		return id;
	}

	void setId(long id) {
		this.id = id;
	}

	public long getTrigid() {
		return trigid;
	}

	void setTrigid(long tbid) {
		this.trigid = tbid;
	}

	public TrigMeta getStrcut() {
		return meta;
	}

	void setStrcut(TrigMeta meta) {
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

	public boolean isDisabled() {
		if (disabled == 1) {
			return true;
		} else {
			return false;
		}
	}

	void setDisabled(boolean disabled) {
		if (disabled) {
			this.disabled = 1;
		} else {
			this.disabled = 0;
		}
	}

	public Date getCt() {
		return ct;
	}

	void setCt(Date ct) {
		this.ct = ct;
	}

}
