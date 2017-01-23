package com.caijun.em.monitor.dbschema;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;

import com.caijun.em.Area;
import com.caijun.em.Systore;
import com.caijun.em.monitor.dbnet.DBInfo;
import com.caijun.utils.str.StringUtil;
import com.tongtech.cj.dbMeta.TrigMeta;
import com.tongtech.cj.dbMeta.TrigMetaComparer;

public class TrigMetaDetector {
	private Logger logger = Logger.getLogger("dbschema");
	private Systore systore;
	private Map<Long, TrigStatus> curStatus;

	public TrigMetaDetector(Systore systore, Map<Long, TrigStatus> curTrigStatus) {
		super();
		this.systore = systore;
		curStatus = curTrigStatus;
	}

	public void sampling(boolean withStruct) {
		List<TrigStatus> trigststus = new ArrayList<TrigStatus>();
		List<TrigInfo> trigInfos = new ArrayList<TrigInfo>();

		for (TrigInfo trigInfo : systore.dbSchema.getTrigInfos()) {
			if (!systore.dbNet.isDisconn(trigInfo.getDbid())) {
				trigInfos.add(trigInfo);
			}
		}

		if (logger.isDebugEnabled()) {
			for (DBInfo disconn : systore.dbNet.getAllDisconnDBInfo()) {
				Area area = systore.getArea(disconn.getAid());
				logger.debug("[" + area.getName() + "]数据库[" + disconn.getUrl() + "]连接异常,不能获得触发器结构状态.");
			}
		}

		ExecutorService threadPool = Executors.newFixedThreadPool(8);
		CompletionService<TrigStatus> completionService = new ExecutorCompletionService<TrigStatus>(threadPool);
		for (TrigInfo TrigInfo : trigInfos) {
			completionService.submit(new TrigStatusGetor(TrigInfo, withStruct));
		}
		try {
			for (int i = 0; i < trigInfos.size(); i++) {
				TrigStatus TrigStatus = completionService.take().get();
				if (TrigStatus != null) {
					trigststus.add(TrigStatus);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		threadPool.shutdown();
		saveTrigStatus(trigststus, withStruct);
	}

	public TrigStatus TrigStatusGetor(TrigInfo trigInfo, boolean withStruct) {
		try {
			boolean dropped = false;
			boolean disabled = false;
			String result = systore.dbNet.getJdbc(trigInfo.getDbid()).query(
					"select status from all_triggers where owner='" + StringUtil.toUpperCase(trigInfo.getSchema())
							+ "' and NLS_UPPER(table_name)='" + StringUtil.toUpperCase(trigInfo.getTbName())
							+ "' and NLS_UPPER(trigger_name)='" + StringUtil.toUpperCase(trigInfo.getTrigName()) + "'",
					new ResultSetExtractor<String>() {
						@Override
						public String extractData(ResultSet rs) throws SQLException, DataAccessException {
							if (rs.next()) {
								return rs.getString(1);
							}
							return null;
						}
					});
			if (result == null) {
				dropped = true;
				disabled = true;
			} else if (!result.equals("ENABLED")) {
				disabled = true;
			}

			TrigStatus trigStatus = new TrigStatus();
			if (!dropped && withStruct) {
				trigStatus.setStrcut(new TrigMeta(trigInfo.getSchema(), trigInfo.getTrigName(),
						systore.dbNet.getDS(trigInfo.getDbid())));
			}
			trigStatus.setTrigid(trigInfo.getId());
			trigStatus.setDroped(dropped);
			trigStatus.setDisabled(disabled);
			trigStatus.setCt(new java.sql.Timestamp(new Date().getTime()));
			trigStatus.setId(systore.id.getNext());
			return trigStatus;
		} catch (Exception e) {
			logger.error("[" + trigInfo.getDbid() + "]不能获得触发器[" + trigInfo.getSchema() + "." + trigInfo.getTbName()
					+ "." + trigInfo.getTrigName() + "]结构状态.", e);
			return null;
		}
	}

	private void saveTrigStatus(List<TrigStatus> list, boolean withStruct) {
		final List<TrigStatus> newStatus = new ArrayList<TrigStatus>();
		for (TrigStatus a : list) {
			TrigStatus b = curStatus.get(a.getTrigid());
			if (b == null || b.isDroped() != a.isDroped() || b.isDisabled() != a.isDisabled()) {
				if (!withStruct && b != null) {
					a.setStrcut(b.getStrcut());
				}
				curStatus.put(a.getTrigid(), a);
				newStatus.add(a);
			} else if (withStruct && !TrigMetaComparer.compare(a.getStrcut(), b.getStrcut())) {
				curStatus.put(a.getTrigid(), a);
				newStatus.add(a);
			}
		}

		if (newStatus.size() == 0) {
			return;
		}

		systore.jdbc.batchUpdate("insert into trigerstatus(id,trigid,struct,disabled,droped,ct) values(?,?,?,?,?,?)",
				new BatchPreparedStatementSetter() {
					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						ps.setLong(1, newStatus.get(i).getId());
						ps.setLong(2, newStatus.get(i).getTrigid());
						TrigMeta meta = newStatus.get(i).getStrcut();
						if (meta == null) {
							ps.setString(3, null);
						} else {
							ps.setString(3, meta.toXML());
						}
						ps.setInt(4, newStatus.get(i).isDisabled() ? 1 : 0);
						ps.setInt(5, newStatus.get(i).isDroped() ? 1 : 0);
						ps.setTimestamp(6, new java.sql.Timestamp(newStatus.get(i).getCt().getTime()));
					}

					@Override
					public int getBatchSize() {
						return newStatus.size();
					}
				});

	}

	private final class TrigStatusGetor implements Callable<TrigStatus> {
		private TrigInfo TrigInfo;
		private boolean withStruct;

		private TrigStatusGetor(TrigInfo TrigInfo, boolean withStruct) {
			super();
			this.TrigInfo = TrigInfo;
			this.withStruct = withStruct;
		}

		@Override
		public TrigStatus call() throws Exception {
			return TrigStatusGetor(TrigInfo, withStruct);
		}

	}
}
