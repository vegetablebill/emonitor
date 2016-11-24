package com.caijun.em.monitor.dbnet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import com.caijun.em.Systore;

public class DBNetDetector {
	private Logger logger = Logger.getLogger("dbnet");
	private Systore systore;
	private Map<Long, DBStatus> curStatus;

	public DBNetDetector(Systore systore, Map<Long, DBStatus> curStatus) {
		super();
		this.systore = systore;
		this.curStatus = curStatus;
	}

	public void sampling() {
		List<DBInfo> dbinfos = systore.dbNet.getDBInfos();
		List<DBStatus> dbststus = new ArrayList<DBStatus>();
		ExecutorService threadPool = Executors.newFixedThreadPool(8);
		CompletionService<DBStatus> completionService = new ExecutorCompletionService<DBStatus>(
				threadPool);
		for (DBInfo dbInfo : dbinfos) {
			completionService.submit(new PingDB(dbInfo));
		}
		try {
			for (int i = 0; i < dbinfos.size(); i++) {
				dbststus.add(completionService.take().get());
			}
		} catch (Exception e) {
			logger.error(e);
		}
		threadPool.shutdown();
		saveDBStatus(dbststus);
	}

	private DBStatus pingDB(DBInfo dbInfo) {
		DataSource ds = systore.dbNet.getDS(dbInfo.getId());
		Connection conn = null;
		Statement statement = null;
		ResultSet rs = null;
		DBStatus dbStatus = new DBStatus();
		dbStatus.setId(systore.id.getNext());
		dbStatus.setDbid(dbInfo.getId());
		try {
			conn = ds.getConnection();
			statement = conn.createStatement();
			rs = statement.executeQuery("select 1 from dual");
			if (rs.next()) {
				dbStatus.setDisconn(false);
			}
		} catch (Exception e) {
			dbStatus.setDisconn(true);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
				}
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
				}
			}
		}
		dbStatus.setCt(new Date());
		return dbStatus;
	}

	private void saveDBStatus(List<DBStatus> list) {
		final List<DBStatus> newStatus = new ArrayList<DBStatus>();
		for (DBStatus a : list) {
			DBStatus b = curStatus.get(a.getDbid());
			if (b == null || b.isDisconn() != a.isDisconn()) {
				curStatus.put(a.getDbid(), a);
				newStatus.add(a);
			}
		}
		if (newStatus.size() == 0) {
			return;
		}
		systore.jdbc.batchUpdate(
				"insert into dbstatus(id,dbid,disconn,ct) values(?,?,?,?)",
				new BatchPreparedStatementSetter() {
					@Override
					public void setValues(PreparedStatement ps, int i)
							throws SQLException {
						ps.setLong(1, newStatus.get(i).getId());
						ps.setLong(2, newStatus.get(i).getDbid());
						ps.setInt(3, newStatus.get(i).isDisconn() ? 1 : 0);
						ps.setTimestamp(4, new java.sql.Timestamp(newStatus
								.get(i).getCt().getTime()));
					}

					@Override
					public int getBatchSize() {
						return newStatus.size();
					}
				});

	}

	private final class PingDB implements Callable<DBStatus> {
		private DBInfo dbInfo;

		private PingDB(DBInfo dbInfo) {
			super();
			this.dbInfo = dbInfo;
		}

		@Override
		public DBStatus call() throws Exception {
			return pingDB(dbInfo);
		}

	}
}
