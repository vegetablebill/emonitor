package com.caijun.em.monitor.dbschema;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;

import com.caijun.em.IDGen;
import com.caijun.em.monitor.dbnet.DBNetMag;
import com.caijun.utils.str.StringUtil;
import com.tongtech.cj.dbMeta.DBMetaException;
import com.tongtech.cj.dbMeta.TBMeta;
import com.tongtech.cj.dbMeta.TrigMeta;

public class DBSchemaMag {
	private Logger logger = Logger.getLogger("dbschema");
	private JdbcTemplate jdbc;
	private IDGen id;
	private DBNetMag dbNet;
	private Map<Long, TBInfo> tbInfosCache;
	private Map<Long, TrigInfo> trigInfosCache;
	private Map<Long, TBStatus> curTBStatus;
	private Map<Long, TrigStatus> curTrigStatus;

	public DBSchemaMag(JdbcTemplate jdbc, IDGen id, DBNetMag dbNet, Map<Long, TBInfo> tbInfos,
			Map<Long, TrigInfo> trigInfos, Map<Long, TBStatus> curTBStatus, Map<Long, TrigStatus> curTrigStatus) {
		super();
		this.jdbc = jdbc;
		this.id = id;
		this.dbNet = dbNet;
		tbInfosCache = tbInfos;
		trigInfosCache = trigInfos;
		this.curTBStatus = curTBStatus;
		this.curTrigStatus = curTrigStatus;

		long begin = new Date().getTime();
		logger.debug("[DBSchemaMag]开始加载缓存...");
		load_tbinfosCache();
		load_triginfosCache();
		load_tbstatusCache();
		load_trigstatusCache();
		long end = new Date().getTime();
		double time = (end - begin) / 1000.0;
		logger.debug("[DBSchemaMag]加载缓存完成,用时:" + time + "s");

	}

	public List<TBInfo> getTBInfos() {
		return new ArrayList<TBInfo>(tbInfosCache.values());
	}

	public List<TBInfo> getTBInfosOfDB(long dbid) {
		List<TBInfo> rs = new ArrayList<TBInfo>();
		for (TBInfo tbInfo : tbInfosCache.values()) {
			if (tbInfo.getDbid() == dbid) {
				rs.add(tbInfo);
			}
		}
		return rs;
	}

	public List<TrigInfo> getTrigInfos() {
		return new ArrayList<TrigInfo>(trigInfosCache.values());
	}

	public TBInfo getTBInfo(long id) {
		return tbInfosCache.get(id);
	}

	public TBInfo getTBInfo(long dbid, String schema, String tbName) {
		schema = StringUtil.toLowerCase(StringUtil.trimDown(schema));
		tbName = StringUtil.toLowerCase(StringUtil.trimDown(tbName));
		for (TBInfo tbInfo : tbInfosCache.values()) {
			if (tbInfo.getDbid() == dbid && StringUtil.equals(tbInfo.getSchema(), schema)
					&& StringUtil.equals(tbInfo.getName(), tbName)) {
				return tbInfo;
			}
		}
		return null;

	}

	public void removeTBInfo(long dbid, String schema, String tbName) {
		schema = StringUtil.toLowerCase(StringUtil.trimDown(schema));
		tbName = StringUtil.toLowerCase(StringUtil.trimDown(tbName));
		jdbc.execute(
				"delete from TBS where dbid=" + dbid + " and tbschema='" + schema + "' and tbname='" + tbName + "'");
		TBInfo tbInfo = getTBInfo(dbid, schema, tbName);
		if (tbInfo != null) {
			tbInfosCache.remove(tbInfo.getId());
		}
	}

	public TrigInfo getTrigInfo(long id) {
		return trigInfosCache.get(id);
	}

	public List<TrigInfo> getTrigInfos(long dbid, String schema, String tbName) {
		schema = StringUtil.toLowerCase(StringUtil.trimDown(schema));
		tbName = StringUtil.toLowerCase(StringUtil.trimDown(tbName));
		List<TrigInfo> result = new ArrayList<TrigInfo>();
		for (TrigInfo trigInfo : trigInfosCache.values()) {
			if (trigInfo.getDbid() == dbid && StringUtil.equals(trigInfo.getSchema(), schema)
					&& StringUtil.equals(trigInfo.getTbName(), tbName)
					&& StringUtil.equals(trigInfo.getTrigName(), tbName)) {
				result.add(trigInfo);
			}
		}
		return result;
	}

	public TrigInfo getTrigInfo(long dbid, String schema, String trigName) {
		schema = StringUtil.toLowerCase(StringUtil.trimDown(schema));
		trigName = StringUtil.toLowerCase(StringUtil.trimDown(trigName));
		for (TrigInfo trigInfo : trigInfosCache.values()) {
			if (trigInfo.getDbid() == dbid && StringUtil.equals(trigInfo.getSchema(), schema)
					&& StringUtil.equals(trigInfo.getTrigName(), trigName)) {
				return trigInfo;
			}
		}
		return null;
	}

	public void removeTrigInfo(long dbid, String schema, String trigName) {
		schema = StringUtil.toLowerCase(StringUtil.trimDown(schema));
		trigName = StringUtil.toLowerCase(StringUtil.trimDown(trigName));
		jdbc.execute("delete from trigers where dbid=" + dbid + "and tschema='" + schema + "' and trigname='" + trigName
				+ "'");
		TrigInfo trigInfo = getTrigInfo(dbid, schema, trigName);
		if (trigInfo != null) {
			trigInfosCache.remove(trigInfo.getId());
		}
	}

	public TBInfo loadTBInfo(long dbid, String schema, String tbName) {
		TBMeta tbMeta;
		try {
			tbMeta = new TBMeta(schema, tbName, dbNet.getDS(dbid));
		} catch (DBMetaException e) {
			logger.error("解析[" + schema + "." + tbName + "]异常.", e);
			removeTBInfo(dbid, schema, tbName);
			return null;
		}
		try {
			TBInfo tbInfo = getTBInfo(dbid, schema, tbName);
			Date ct = new Date();
			if (tbInfo != null) {
				this.jdbc.update("update TBS set dbid=?,tbschema=?,tbname=?, STRUCT=?,ct=? where id = ?",
						new UpdateTBInfoPSS(tbInfo, tbMeta, ct));
				tbInfo.setStrcut(tbMeta);
				tbInfo.setCt(ct);
			} else {
				tbInfo = new TBInfo();
				tbInfo.setId(id.getNext());
				tbInfo.setDbid(dbid);
				tbInfo.setSchema(schema);
				tbInfo.setName(tbName);
				tbInfo.setStrcut(tbMeta);
				tbInfo.setCt(ct);
				this.jdbc.update("insert into TBS(id,dbid,tbschema,tbname,struct,ct) values(?,?,?,?,?,?)",
						new InsertTBInfoPSS(tbInfo));
				tbInfosCache.put(tbInfo.getId(), tbInfo);
			}
			return tbInfo;
		} catch (Exception e) {
			logger.error("保存[" + schema + "." + tbName + "]元信息异常.", e);
			return null;
		}

	}

	public TBStatus getCurTBStatus(long tbid) {
		return curTBStatus.get(tbid);
	}

	public TBStatus getCurTBStatus(TBInfo tbInfo) {
		return curTBStatus.get(tbInfo.getId());
	}

	public boolean isDroped(long tbid) {
		TBStatus TBStatus = getCurTBStatus(tbid);
		return TBStatus.isDroped();
	}

	public boolean isDroped(TBInfo tbInfo) {
		return isDroped(tbInfo.getId());
	}

	public List<TBStatus> getAllCurTBStatus() {
		return new ArrayList<TBStatus>(curTBStatus.values());
	}

	public List<TrigStatus> getAllCurTrigStatus() {
		return new ArrayList<TrigStatus>(curTrigStatus.values());
	}

	public TrigInfo loadTrigInfo(long dbid, String schema, String tbName, String trigName) {
		schema = StringUtil.toLowerCase(StringUtil.trimDown(schema));
		tbName = StringUtil.toLowerCase(StringUtil.trimDown(tbName));
		trigName = StringUtil.toLowerCase(StringUtil.trimDown(trigName));
		TrigMeta trigMeta = null;

		try {
			trigMeta = new TrigMeta(schema, trigName, dbNet.getDS(dbid));
		} catch (DBMetaException e) {
			logger.error("解析[" + schema + "." + tbName + "." + trigName + "]异常.", e);
			removeTrigInfo(dbid, schema, trigName);
			return null;
		}
		try {
			TrigInfo trigInfo = getTrigInfo(dbid, schema, trigName);
			Date ct = new Date();
			if (trigInfo != null) {
				this.jdbc.update("update trigers set dbid=?,tschema=?,tbname=?,trigname=?, STRUCT=?,ct=? where id = ?",
						new UpdateTrigInfoPSS(trigInfo, trigMeta, ct));
				trigInfo.setStrcut(trigMeta);
				trigInfo.setCt(ct);
			} else {
				trigInfo = new TrigInfo();
				trigInfo.setId(id.getNext());
				trigInfo.setDbid(dbid);
				trigInfo.setSchema(schema);
				trigInfo.setTbName(tbName);
				trigInfo.setTrigName(trigName);
				trigInfo.setStrcut(trigMeta);
				trigInfo.setCt(ct);
				this.jdbc.update("insert into trigers(id,dbid,tschema,tbname,trigname,struct,ct) values(?,?,?,?,?,?,?)",
						new InsertTrigInfoPSS(trigInfo));
				trigInfosCache.put(trigInfo.getId(), trigInfo);
			}
			return trigInfo;
		} catch (Exception e) {
			logger.error("保存[" + schema + "." + tbName + "." + trigName + "]元信息异常.", e);
			return null;
		}
	}

	private final class InsertTBInfoPSS implements PreparedStatementSetter {
		TBInfo tbInfo;

		public InsertTBInfoPSS(TBInfo tbInfo) {
			super();
			this.tbInfo = tbInfo;
		}

		@Override
		public void setValues(PreparedStatement ps) throws SQLException {
			ps.setLong(1, tbInfo.getId());
			ps.setLong(2, tbInfo.getDbid());
			ps.setString(3, tbInfo.getSchema());
			ps.setString(4, tbInfo.getName());
			if (tbInfo.getStrcut() == null) {
				ps.setString(5, null);
			} else {
				ps.setString(5, tbInfo.getStrcut().toXML());
			}
			ps.setTimestamp(6, new java.sql.Timestamp(tbInfo.getCt().getTime()));
		}
	}

	private final class UpdateTBInfoPSS implements PreparedStatementSetter {
		TBInfo tbInfo;
		TBMeta meta;
		Date ct;

		public UpdateTBInfoPSS(TBInfo tbInfo, TBMeta meta, Date ct) {
			super();
			this.tbInfo = tbInfo;
			this.meta = meta;
			this.ct = ct;
		}

		@Override
		public void setValues(PreparedStatement ps) throws SQLException {
			ps.setLong(1, tbInfo.getDbid());
			ps.setString(2, tbInfo.getSchema());
			ps.setString(3, tbInfo.getName());
			if (meta == null) {
				ps.setString(4, null);
			} else {
				ps.setString(4, meta.toXML());
			}
			ps.setTimestamp(5, new java.sql.Timestamp(ct.getTime()));
			ps.setLong(6, tbInfo.getId());
		}
	}

	private final class InsertTrigInfoPSS implements PreparedStatementSetter {
		TrigInfo trigInfo;

		public InsertTrigInfoPSS(TrigInfo trigInfo) {
			super();
			this.trigInfo = trigInfo;
		}

		@Override
		public void setValues(PreparedStatement ps) throws SQLException {
			ps.setLong(1, trigInfo.getId());
			ps.setLong(2, trigInfo.getDbid());
			ps.setString(3, trigInfo.getSchema());
			ps.setString(4, trigInfo.getTbName());
			ps.setString(5, trigInfo.getTrigName());
			if (trigInfo.getStrcut() == null) {
				ps.setString(6, null);
			} else {
				ps.setString(6, trigInfo.getStrcut().toXML());
			}
			ps.setTimestamp(7, new java.sql.Timestamp(trigInfo.getCt().getTime()));
		}
	}

	private final class UpdateTrigInfoPSS implements PreparedStatementSetter {
		TrigInfo trigInfo;
		TrigMeta meta;
		Date ct;

		public UpdateTrigInfoPSS(TrigInfo trigInfo, TrigMeta meta, Date ct) {
			super();
			this.trigInfo = trigInfo;
			this.meta = meta;
			this.ct = ct;
		}

		@Override
		public void setValues(PreparedStatement ps) throws SQLException {
			ps.setLong(1, trigInfo.getDbid());
			ps.setString(2, trigInfo.getSchema());
			ps.setString(3, trigInfo.getTbName());
			ps.setString(4, trigInfo.getTrigName());
			if (meta == null) {
				ps.setString(5, null);
			} else {
				ps.setString(5, meta.toXML());
			}
			ps.setTimestamp(6, new java.sql.Timestamp(ct.getTime()));
			ps.setLong(7, trigInfo.getId());
		}
	}

	private void load_tbinfosCache() {
		tbInfosCache.clear();
		List<TBInfo> tbInfos = jdbc.query("select id,dbid,tbschema,tbname,struct,ct from tbs", new TBInfoMapper());
		for (TBInfo tbInfo : tbInfos) {
			tbInfosCache.put(tbInfo.getId(), tbInfo);
		}
	}

	private final class TBInfoMapper implements RowMapper<TBInfo> {
		public TBInfo mapRow(ResultSet rs, int rowNum) {
			TBInfo tbInfo = new TBInfo();
			try {
				tbInfo.setId(rs.getLong("id"));
				tbInfo.setDbid(rs.getLong("dbid"));
				tbInfo.setSchema(StringUtil.toLowerCase(StringUtil.trimDown(rs.getString("tbschema"))));
				tbInfo.setName(StringUtil.toLowerCase(StringUtil.trimDown(rs.getString("tbname"))));
				tbInfo.setCt(new Date(rs.getTimestamp("ct").getTime()));
				String xml = StringUtil.trimDown(rs.getString("struct"));
				if (xml != null) {
					tbInfo.setStrcut(new TBMeta(xml));
				}
			} catch (SQLException e) {
				logger.error(e.getMessage(), e);
			} catch (DBMetaException e) {
				logger.error("解析[" + tbInfo.getSchema() + "." + tbInfo.getName() + "]异常.", e);
			}
			return tbInfo;
		}
	}

	private void load_triginfosCache() {
		trigInfosCache.clear();
		List<TrigInfo> trigInfos = jdbc.query("select id,dbid,tschema ,tbname,trigname,struct,ct from trigers",
				new TrigInfoMapper());
		for (TrigInfo trigInfo : trigInfos) {
			trigInfosCache.put(trigInfo.getId(), trigInfo);
		}
	}

	private final class TrigInfoMapper implements RowMapper<TrigInfo> {
		public TrigInfo mapRow(ResultSet rs, int rowNum) {
			TrigInfo trigInfo = new TrigInfo();
			try {
				trigInfo.setId(rs.getLong("id"));
				trigInfo.setDbid(rs.getLong("dbid"));
				trigInfo.setSchema(StringUtil.toLowerCase(StringUtil.trimDown(rs.getString("tschema"))));
				trigInfo.setTbName(StringUtil.toLowerCase(StringUtil.trimDown(rs.getString("tbname"))));
				trigInfo.setTrigName(StringUtil.toLowerCase(StringUtil.trimDown(rs.getString("trigname"))));
				trigInfo.setCt(new Date(rs.getTimestamp("ct").getTime()));
				String xml = StringUtil.trimDown(rs.getString("struct"));
				if (xml != null) {
					trigInfo.setStrcut(new TrigMeta(xml));
				}
			} catch (SQLException e) {
				logger.error(e.getMessage(), e);
			} catch (DBMetaException e) {
				logger.error("解析[" + trigInfo.getSchema() + "." + trigInfo.getTbName() + "." + trigInfo.getTrigName()
						+ "]异常.", e);
			}
			return trigInfo;
		}
	}

	private void load_tbstatusCache() {
		List<TBStatus> list = jdbc.query("select id,tbid,struct,droped,ct from tbstatus_newly", new TBStatusMapper());
		for (TBStatus TBStatus : list) {
			curTBStatus.put(TBStatus.getTbid(), TBStatus);
		}
	}

	private final class TBStatusMapper implements RowMapper<TBStatus> {
		public TBStatus mapRow(ResultSet rs, int rowNum) {
			TBStatus tbStatus = new TBStatus();
			try {
				tbStatus.setId(rs.getLong("id"));
				tbStatus.setTbid(rs.getLong("tbid"));
				Integer droped = rs.getInt("droped");
				if (droped == null || droped == 0) {
					tbStatus.setDroped(false);
				} else {
					tbStatus.setDroped(true);
				}
				if (!tbStatus.isDroped()) {
					TBMeta meta = new TBMeta(StringUtil.trimDown(rs.getString("struct")));
					tbStatus.setStrcut(meta);
				}
				tbStatus.setCt(rs.getTimestamp("ct"));
			} catch (SQLException e) {
				logger.error(e);
			} catch (DBMetaException e) {
				logger.error(e);
			}
			return tbStatus;
		}
	}

	private void load_trigstatusCache() {
		List<TrigStatus> list = jdbc.query("select id,trigid,struct,disabled,droped,ct from trigerstatus_newly",
				new TrigStatusMapper());
		for (TrigStatus trigStatus : list) {
			curTrigStatus.put(trigStatus.getTrigid(), trigStatus);
		}
	}

	private final class TrigStatusMapper implements RowMapper<TrigStatus> {
		public TrigStatus mapRow(ResultSet rs, int rowNum) {
			TrigStatus trigStatus = new TrigStatus();
			try {
				trigStatus.setId(rs.getLong("id"));
				trigStatus.setTrigid(rs.getLong("trigid"));
				Integer droped = rs.getInt("droped");
				if (droped == null || droped == 0) {
					trigStatus.setDroped(false);
				} else {
					trigStatus.setDroped(true);
				}
				Integer disabled = rs.getInt("disabled");
				if (disabled == null || disabled == 0) {
					trigStatus.setDisabled(false);
				} else {
					trigStatus.setDisabled(true);
				}

				if (!trigStatus.isDroped()) {
					TrigMeta meta = new TrigMeta(StringUtil.trimDown(rs.getString("struct")));
					trigStatus.setStrcut(meta);
				}
				trigStatus.setCt(rs.getTimestamp("ct"));
			} catch (SQLException e) {
				logger.error(e);
			} catch (DBMetaException e) {
				logger.error(e);
			}
			return trigStatus;
		}
	}
}
