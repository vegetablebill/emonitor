package com.caijun.em.monitor.dbschema;

import java.util.Date;

import com.caijun.utils.str.StringUtil;
import com.tongtech.cj.dbMeta.TBMeta;

public class TBInfo {
	private long id;
	private long dbid;
	private String schema;
	private String name;
	private TBMeta meta;
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

	public String getName() {
		return name;
	}

	void setName(String name) {
		this.name = StringUtil.toLowerCase(StringUtil.trimDown(name));
	}

	public TBMeta getStrcut() {
		return meta;
	}

	void setStrcut(TBMeta strcut) {
		this.meta = strcut;
	}

	public Date getCt() {
		return ct;
	}

	void setCt(Date ct) {
		this.ct = ct;
	}
	
}
