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

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DBNetMag {
	private Logger logger = Logger.getLogger("dbnet");
	private JdbcTemplate jdbc;
	private Map<Long, DBStatus> curStatus;
	private Map<Long, DBInfo> dbInfosCache;
	private Map<Long, DataSource> dscache;
	private Map<Long, JdbcTemplate> jdbcCache;

	public DBNetMag(JdbcTemplate jdbc, Map<Long, DBStatus> curStatus,
			Map<Long, DBInfo> dbInfos, Map<Long, DataSource> dss,
			Map<Long, JdbcTemplate> jdbcs) {
		super();
		this.jdbc = jdbc;
		dbInfosCache = dbInfos;
		dscache = dss;
		jdbcCache = jdbcs;
		this.curStatus = curStatus;

		logger.debug("[DBNetMag]��ʼ���ػ���...");
		long begin = new Date().getTime();
		load_dbInfosCache();
		load_dbdssCache();
		load_curdbstatus();
		long end = new Date().getTime();
		double time = (end - begin) / 1000.0;
		logger.debug("[DBNetMag]���ػ������,��ʱ:" + time + "s");

	}

	public List<DBInfo> getDBInfos() {
		return new ArrayList<DBInfo>(dbInfosCache.values());
	}

	public DBInfo getDBInfo(long id) {
		return dbInfosCache.get(id);
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

	public boolean isDisconn(DBInfo dbInfo) {
		return isDisconn(dbInfo.getId());
	}

	public List<DBStatus> getAllCurDBStatus() {
		return new ArrayList<DBStatus>(curStatus.values());
	}

	public List<DBStatus> getAllDisconnDBStatus() {
		List<DBStatus> result = new ArrayList<DBStatus>();
		for (DBStatus dbStatus : curStatus.values()) {
			if (dbStatus.isDisconn()) {
				result.add(dbStatus);
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
		List<DBInfo> dbInfos = jdbc.query(
				"select id,aid,url,usr,pasword from dbs", new DBInfoMapper());
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
		List<DBStatus> list = jdbc.query(
				"select id,dbid,disconn,ct from dbstatus_newly",
				new DBStatusMapper());
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