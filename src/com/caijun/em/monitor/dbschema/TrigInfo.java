package com.caijun.em.monitor.dbschema;

import java.util.Date;

import com.caijun.utils.str.StringUtil;
import com.tongtech.cj.dbMeta.TrigMeta;

public class TrigInfo {
	private long id;
	private long dbid;
	private String schema;
	private String tbName;
	private String trigName;
	private TrigMeta meta;
	private Date ct;

	public long getId() {
		return id;
	}

	void setId(long id) {
		this.id = id;
	}

	public long getDbid() {
		return dbid;
	}

	void setDbid(long dbid) {
		this.dbid = dbid;
	}

	public String getSchema() {
		return schema;
	}

	void setSchema(String schema) {
		this.schema = StringUtil.toLowerCase(StringUtil.trimDown(schema));
	}

	public String getTbName() {
		return tbName;
	}

	void setTbName(String tbName) {
		this.tbName = StringUtil.toLowerCase(StringUtil.trimDown(tbName));
	}

	public String getTrigName() {
		return trigName;
	}

	void setTrigName(String trigName) {
		this.trigName = StringUtil.toLowerCase(StringUtil.trimDown(trigName));
	}

	public TrigMeta getStrcut() {
		return meta;
	}

	void setStrcut(TrigMeta strcut) {
		this.meta = strcut;
	}

	public Date getCt() {
		return ct;
	}

	void setCt(Date ct) {
		this.ct = ct;
	}

}
