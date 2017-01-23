package com.caijun.em.monitor.dbnet;

import java.beans.PropertyVetoException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.caijun.em.Props;
import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DBNetMag {
	private static String CENTER_ID = "area.centerID";
	private Logger logger = Logger.getLogger("dbnet");
	private JdbcTemplate jdbc;
	private Props props;
	private Map<Long, DBStatus> curStatus;
	private Map<Long, DBInfo> dbInfosCache;
	private Map<Long, DataSource> dscache;
	private Map<Long, JdbcTemplate> jdbcCache;
	private DBInfo centerDBInfo;
	private long centerAreaID = -1L;

	public DBNetMag(JdbcTemplate jdbc, Props props, Map<Long, DBStatus> curStatus, Map<Long, DBInfo> dbInfos,
			Map<Long, DataSource> dss, Map<Long, JdbcTemplate> jdbcs) {
		super();
		this.jdbc = jdbc;
		this.props = props;
		dbInfosCache = dbInfos;
		dscache = dss;
		jdbcCache = jdbcs;
		this.curStatus = curStatus;

		logger.debug("[DBNetMag]开始加载缓存...");
		long begin = new Date().getTime();
		load_dbInfosCache();
		load_dbdssCache();
		load_curdbstatus();
		long end = new Date().getTime();
		double time = (end - begin) / 1000.0;
		logger.debug("[DBNetMag]加载缓存完成,用时:" + time + "s");

	}

	public List<DBInfo> getDBInfos() {
		return new ArrayList<DBInfo>(dbInfosCache.values());
	}

	public DBInfo getDBInfo(long id) {
		return dbInfosCache.get(id);
	}

	public DBInfo getCenterDBInfo() {
		long centerAreaID = props.get(CENTER_ID, 0);
		if (this.centerAreaID != centerAreaID) {
			for (DBInfo dbInfo : dbInfosCache.values()) {
				if (centerAreaID == dbInfo.getAid()) {
					centerDBInfo = dbInfo;
					this.centerAreaID = centerAreaID;
					break;
				}
			}
		}
		return centerDBInfo;
	}

	public DataSource getDS(long dbid) {
		return dscache.get(dbid);
	}

	public JdbcTemplate getJdbc(DBInfo dbInfo) {
		return getJdbc(dbInfo.getId());
	}

	public JdbcTemplate getJdbc(long dbid) {
		return jdbcCache.get(dbid);
	}

	public DataSource getDS(DBInfo dbInfo) {
		return getDS(dbInfo.getId());
	}

	public DBStatus getCurDBStatus(long dbid) {
		return curStatus.get(dbid);
	}

	public DBStatus getCurDBStatus(DBInfo dbInfo) {
		return curStatus.get(dbInfo.getId());
	}

	public boolean isDisconn(long dbid) {
		DBStatus dbStatus = getCurDBStatus(dbid);
		return dbStatus.isDisconn();
	}

	public boolean centerIsDisconn() {
		DBStatus dbStatus = getCurDBStatus(getCenterDBInfo());
		return dbStatus.isDisconn();
	}

	public boolean isDisconn(DBInfo dbInfo) {
		return isDisconn(dbInfo.getId());
	}

	public List<DBStatus> getAllCurDBStatus() {
		return new ArrayList<DBStatus>(curStatus.values());
	}

	public List<DBInfo> getAllDisconnDBInfo() {
		List<DBInfo> result = new ArrayList<DBInfo>();
		for (DBStatus dbStatus : curStatus.values()) {
			if (dbStatus.isDisconn()) {
				result.add(this.getDBInfo(dbStatus.getDbid()));
			}
		}
		return result;
	}

	public List<DBInfo> getAllConnDBInfo() {
		List<DBInfo> result = new ArrayList<DBInfo>();
		for (DBStatus dbStatus : curStatus.values()) {
			if (!dbStatus.isDisconn()) {
				result.add(this.getDBInfo(dbStatus.getDbid()));
			}
		}
		return result;
	}

	private void load_dbdssCache() {
		dscache.clear();
		jdbcCache.clear();
		List<DBInfo> dbInfos = getDBInfos();
		for (DBInfo dbInfo : dbInfos) {
			DataSource ds = loadDS(dbInfo);
			dscache.put(dbInfo.getId(), ds);
			jdbcCache.put(dbInfo.getId(), new JdbcTemplate(ds));
		}

	}

	private DataSource loadDS(DBInfo dbInfo) {
		ComboPooledDataSource dataSource = new ComboPooledDataSource();
		try {
			dataSource.setDriverClass("oracle.jdbc.driver.OracleDriver");
			dataSource.setJdbcUrl(dbInfo.getUrl());
			dataSource.setUser(dbInfo.getUsr());
			dataSource.setPassword(dbInfo.getPassword());
			dataSource.setAcquireRetryAttempts(1);
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
		return dataSource;
	}

	private void load_dbInfosCache() {
		dbInfosCache.clear();
		List<DBInfo> dbInfos = jdbc.query("select id,aid,url,usr,pasword from dbs", new DBInfoMapper());
		for (DBInfo dbInfo : dbInfos) {
			dbInfosCache.put(dbInfo.getId(), dbInfo);
		}
	}

	private final class DBInfoMapper implements RowMapper<DBInfo> {
		public DBInfo mapRow(ResultSet rs, int rowNum) throws SQLException {
			DBInfo dbInfo = new DBInfo();
			dbInfo.setId(rs.getLong("id"));
			dbInfo.setAid(rs.getLong("aid"));
			String url = rs.getString("url");
			url = url == null ? null : url.trim();
			dbInfo.setUrl(url);
			String usr = rs.getString("usr");
			usr = usr == null ? null : usr.trim();
			dbInfo.setUsr(usr);
			String pasword = rs.getString("pasword");
			pasword = pasword == null ? null : pasword.trim();
			dbInfo.setPassword(pasword);
			return dbInfo;
		}
	}

	private void load_curdbstatus() {
		List<DBStatus> list = jdbc.query("select id,dbid,disconn,ct from dbstatus_newly", new DBStatusMapper());
		for (DBStatus dbStatus : list) {
			curStatus.put(dbStatus.getDbid(), dbStatus);
		}
	}

	private final class DBStatusMapper implements RowMapper<DBStatus> {
		public DBStatus mapRow(ResultSet rs, int rowNum) throws SQLException {
			DBStatus dbstatus = new DBStatus();
			dbstatus.setId(rs.getLong("id"));
			dbstatus.setDbid(rs.getLong("dbid"));
			Integer disconn = rs.getInt("disconn");
			if (disconn == null || disconn == 0) {
				dbstatus.setDisconn(false);
			} else {
				dbstatus.setDisconn(true);
			}
			dbstatus.setCt(rs.getTimestamp("ct"));
			return dbstatus;
		}
	}
}
