package com.caijun.em.monitor.dbschema;

import java.sql.PreparedStatement;
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
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

import com.caijun.em.Area;
import com.caijun.em.Systore;
import com.caijun.em.monitor.dbnet.DBInfo;
import com.caijun.utils.str.StringUtil;
import com.tongtech.cj.dbMeta.TBMeta;
import com.tongtech.cj.dbMeta.TBMetaComparer;
import com.tongtech.cj.dbMeta.TBMetaComparer.CompareCOLSRS;

public class TBMetaDetector {
	private Logger logger = Logger.getLogger("dbschema");
	private Systore systore;
	private Map<Long, TBStatus> curStatus;

	public TBMetaDetector(Systore systore, Map<Long, TBStatus> curTBStatus) {
		super();
		this.systore = systore;
		curStatus = curTBStatus;
	}

	public void sampling() {
		List<TBStatus> tbststus = new ArrayList<TBStatus>();
		List<TBInfo> tbInfos = new ArrayList<TBInfo>();

		for (TBInfo tbInfo : systore.dbSchema.getTBInfos()) {
			if (!systore.dbNet.isDisconn(tbInfo.getDbid())) {
				tbInfos.add(tbInfo);
			}
		}

		if (logger.isDebugEnabled()) {
			for (DBInfo disconn : systore.dbNet.getAllDisconnDBInfo()) {
				Area area = systore.getArea(disconn.getAid());
				logger.debug("[" + area.getName() + "]数据库[" + disconn.getUrl() + "]连接异常,不能获得表结构状态.");
			}
		}

		ExecutorService threadPool = Executors.newFixedThreadPool(8);
		CompletionService<TBStatus> completionService = new ExecutorCompletionService<TBStatus>(threadPool);
		for (TBInfo tbInfo : tbInfos) {
			completionService.submit(new TBStatusGetor(tbInfo));
		}
		try {
			for (int i = 0; i < tbInfos.size(); i++) {
				TBStatus tbStatus = completionService.take().get();
				if (tbStatus != null) {
					tbststus.add(tbStatus);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		threadPool.shutdown();
		saveTBStatus(tbststus);
	}

	public TBStatus tbStatusGetor(TBInfo tbInfo) {
		try {
			boolean dropped = false;
			Integer result = systore.dbNet.getJdbc(tbInfo.getDbid()).queryForObject(
					"select count(1) from all_tables where owner='" + StringUtil.toUpperCase(tbInfo.getSchema())
							+ "' and NLS_UPPER(table_name)='" + StringUtil.toUpperCase(tbInfo.getName()) + "'",
					Integer.class);
			if (result == 0) {
				dropped = true;
			}
			TBStatus tbStatus = new TBStatus();
			if (!dropped) {
				tbStatus.setStrcut(
						new TBMeta(tbInfo.getSchema(), tbInfo.getName(), systore.dbNet.getDS(tbInfo.getDbid())));
			}
			tbStatus.setTbid(tbInfo.getId());
			tbStatus.setDroped(dropped);
			tbStatus.setCt(new java.sql.Timestamp(new Date().getTime()));
			tbStatus.setId(systore.id.getNext());
			return tbStatus;
		} catch (Exception e) {
			logger.error("[" + tbInfo.getDbid() + "]不能获得表[" + tbInfo.getSchema() + "." + tbInfo.getName() + "]结构状态.",
					e);
			return null;
		}
	}

	private void saveTBStatus(List<TBStatus> list) {
		final List<TBStatus> newStatus = new ArrayList<TBStatus>();
		for (TBStatus a : list) {
			TBStatus b = curStatus.get(a.getTbid());
			if (b == null || b.isDroped() != a.isDroped()) {
				curStatus.put(a.getTbid(), a);
				newStatus.add(a);
			} else {
				CompareCOLSRS result = TBMetaComparer.compareCols(a.getStrcut(), b.getStrcut(), true, true);
				if (result.getDiff().size() > 0 || result.getOnlyleft().size() > 0
						|| result.getOnlyRight().size() > 0) {
					curStatus.put(a.getTbid(), a);
					newStatus.add(a);
				}
			}

		}

		if (newStatus.size() == 0) {
			return;
		}

		systore.jdbc.batchUpdate("insert into TBStatus(id,tbid,struct,droped,ct) values(?,?,?,?,?)",
				new BatchPreparedStatementSetter() {
					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						ps.setLong(1, newStatus.get(i).getId());
						ps.setLong(2, newStatus.get(i).getTbid());
						TBMeta meta = newStatus.get(i).getStrcut();
						if (meta == null) {
							ps.setString(3, null);
						} else {
							ps.setString(3, meta.toXML());
						}
						ps.setInt(4, newStatus.get(i).isDroped() ? 1 : 0);
						ps.setTimestamp(5, new java.sql.Timestamp(newStatus.get(i).getCt().getTime()));
					}

					@Override
					public int getBatchSize() {
						return newStatus.size();
					}
				});

	}

	private final class TBStatusGetor implements Callable<TBStatus> {
		private TBInfo tbInfo;

		private TBStatusGetor(TBInfo tbInfo) {
			super();
			this.tbInfo = tbInfo;
		}

		@Override
		public TBStatus call() throws Exception {
			return tbStatusGetor(tbInfo);
		}

	}
}
