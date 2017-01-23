package com.caijun.em.monitor.dbdata;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

import com.caijun.em.Area;
import com.caijun.em.Systore;
import com.caijun.em.monitor.dbnet.DBInfo;
import com.caijun.em.monitor.dbschema.TBInfo;
import com.caijun.utils.str.StringUtil;

public class TBDataDetector {
	private Logger logger = Logger.getLogger("tbdata");
	private Systore systore;
	private Map<Long, TBDataStatus> curTBDataStatus;

	public TBDataDetector(Systore syStore, Map<Long, TBDataStatus> curTBDataStatus) {
		super();
		this.systore = syStore;
		this.curTBDataStatus = curTBDataStatus;
		long begin = new Date().getTime();
		logger.debug("[TBDataDetector]开始加载缓存...");
		load_TBDataCache();
		long end = new Date().getTime();
		double time = (end - begin) / 1000.0;
		logger.debug("[TBDataDetector]加载缓存完成,用时:" + time + "s");
	}

	@SuppressWarnings("rawtypes")
	public void sampling() {
		List<TBInfo> tbInfos = new ArrayList<TBInfo>();
		DBInfo centerDBInfo = systore.dbNet.getCenterDBInfo();
		for (TBInfo tbInfo : systore.dbSchema.getTBInfos()) {
			if (!systore.dbNet.isDisconn(tbInfo.getDbid()) && centerDBInfo.getId() != tbInfo.getDbid()) {
				tbInfos.add(tbInfo);
			}
		}
		if (logger.isDebugEnabled()) {
			for (DBInfo disconn : systore.dbNet.getAllDisconnDBInfo()) {
				Area area = systore.getArea(disconn.getAid());
				logger.debug("[" + area.getName() + "]数据库[" + disconn.getUrl() + "]连接异常,不能获得表的数据量状态.");
			}
		}

		ExecutorService threadPool = Executors.newFixedThreadPool(8);
		Future<Map<String, Map<String, Long>>> future_center = threadPool.submit(new CenterSumsGetor());
		List<Future<Map<String, List>>> futures_needDeleteSum = new ArrayList<Future<Map<String, List>>>();
		for (DBInfo conn : systore.dbNet.getAllConnDBInfo()) {
			futures_needDeleteSum.add(threadPool.submit(new NeedDeleteSumGetor(conn.getId())));
		}
		List<Future<List>> futures_tbDataSum = new ArrayList<Future<List>>();
		for (TBInfo tbInfo : tbInfos) {
			futures_tbDataSum.add(threadPool.submit(new TBDataSumGetor(tbInfo)));
		}

		List<TBDataStatus> tbDataStatuses = new ArrayList<TBDataStatus>();
		Date ct = new Date();
		try {
			Map<String, Map<String, Long>> centerSums = future_center.get();
			Map<String, List> needDeleteSums = new HashMap<String, List>();
			Map<String, List> tbDataSums = new HashMap<String, List>();
			for (Future<Map<String, List>> future_needDeleteSum : futures_needDeleteSum) {
				needDeleteSums.putAll(future_needDeleteSum.get());
			}
			for (Future<List> future_tbDataSum : futures_tbDataSum) {
				List ls = future_tbDataSum.get();
				tbDataSums.put(ls.get(0).toString() + ls.get(1).toString(), ls);
			}

			for (TBInfo tbInfo : tbInfos) {
				List needDeleteSum = needDeleteSums.get(tbInfo.getDbid() + tbInfo.getName());
				List tbDataSum = tbDataSums.get(tbInfo.getDbid() + tbInfo.getName());
				tbDataStatuses
						.add(new TBDataStatus(systore.id.getNext(), (long) tbInfo.getId(), (long) tbDataSum.get(2),
								(long) tbDataSum.get(3), (Date) tbDataSum.get(4), (long) needDeleteSum.get(2),
								(Date) needDeleteSum.get(3),
								centerSums.get(tbInfo.getName()).get(
										systore.getArea(systore.dbNet.getDBInfo(tbInfo.getDbid()).getAid()).getAid()),
								ct));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		threadPool.shutdown();
		saveTBDataStatus(tbDataStatuses);
	}

	private Map<String, Map<String, Long>> getCenterSums() {
		Map<String, Map<String, Long>> centerSums = new HashMap<String, Map<String, Long>>();
		DBInfo centerDBInfo = systore.dbNet.getCenterDBInfo();
		long centerDBid = centerDBInfo.getId();
		JdbcTemplate jdbc = systore.dbNet.getJdbc(centerDBInfo);
		final Set<String> areaCodes = new HashSet<String>();
		Set<String> tbNames = new HashSet<String>();
		for (Area area : systore.getAreas()) {
			if (area.getAid() != null) {
				areaCodes.add(area.getAid());
			}
		}
		for (TBInfo tbInfo : systore.dbSchema.getTBInfos()) {
			if (tbInfo.getDbid() == centerDBid) {
				continue;
			}
			tbNames.add(tbInfo.getName());
		}
		for (String tbName : tbNames) {
			try {
				Map<String, Long> map = jdbc.query(
						"select source_areaid as areaid,count(1) as sum from " + tbName + " group by source_areaid",
						new ResultSetExtractor<Map<String, Long>>() {
							@Override
							public Map<String, Long> extractData(ResultSet rs)
									throws SQLException, DataAccessException {
								Map<String, Long> map = new HashMap<String, Long>();
								while (rs.next()) {
									String areaid = rs.getString("areaid");
									if (areaCodes.contains(areaid)) {
										map.put(rs.getString("areaid"), rs.getLong("sum"));
									}
								}
								return map;
							}
						});
				for (String areaCode : areaCodes) {
					if (!map.containsKey(areaCode)) {
						map.put(areaCode, 0L);
					}
				}
				centerSums.put(tbName, map);
			} catch (Exception e) {
				logger.error(tbName,e);
				Map<String, Long> map = new HashMap<String, Long>();
				for (String areaCode : areaCodes) {
					map.put(areaCode, -1L);
				}
				centerSums.put(tbName, map);
			}
		}
		return centerSums;
	}

	private final class CenterSumsGetor implements Callable<Map<String, Map<String, Long>>> {
		@Override
		public Map<String, Map<String, Long>> call() throws Exception {
			return getCenterSums();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map<String, List> getNeedDeleteSum(final Long dbID) {
		Map<String, List> rs = null;
		try {
			rs = systore.dbNet.getJdbc(dbID).query(
					"select tbname,count(1) as sum ,min(tong_time) as tong_time from tong_temp group by tbname ,tbschema",
					new ResultSetExtractor<Map<String, List>>() {
						@Override
						public Map<String, List> extractData(ResultSet rs) throws SQLException, DataAccessException {
							Map<String, List> rs_temp = new HashMap<String, List>();
							while (rs.next()) {
								List list = new ArrayList();
								list.add(dbID);
								list.add(StringUtil.toLowerCase(rs.getString("tbname")));
								list.add(rs.getLong("sum"));
								list.add(rs.getTimestamp("tong_time"));
								rs_temp.put(dbID + (String) list.get(1), list);
							}
							return rs_temp;
						}
					});
			for (TBInfo tbInfo : systore.dbSchema.getTBInfosOfDB(dbID)) {
				if (!rs.containsKey(dbID + tbInfo.getName())) {
					List list = new ArrayList();
					list.add(dbID);
					list.add(tbInfo.getName());
					list.add(0L);
					list.add(null);
					rs.put(dbID + tbInfo.getName(), list);
				}
			}
		} catch (Exception e) {
			logger.error(e);
			rs = new HashMap<String, List>();
			for (TBInfo tbInfo : systore.dbSchema.getTBInfosOfDB(dbID)) {
				List list = new ArrayList();
				list.add(dbID);
				list.add(tbInfo.getName());
				list.add(-1L);
				list.add(null);
				rs.put(dbID + tbInfo.getName(), list);
			}
		}
		return rs;
	}

	@SuppressWarnings("rawtypes")
	private final class NeedDeleteSumGetor implements Callable<Map<String, List>> {
		private long dbID;

		public NeedDeleteSumGetor(long dbID) {
			super();
			this.dbID = dbID;
		}

		@Override
		public Map<String, List> call() throws Exception {
			return getNeedDeleteSum(dbID);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private List getTBDataSum(final TBInfo tbInfo) {
		List rs = null;
		try {
			rs = systore.dbNet.getJdbc(tbInfo.getDbid())
					.query("select count(1) sum, count(case when tongflag > 0 THEN 1 end) csum, min(case when tongflag > 0 THEN tong_time end) cmintime from "
							+ tbInfo.getName(), new ResultSetExtractor<List>() {
								@Override
								public List extractData(ResultSet rs) throws SQLException, DataAccessException {
									List list = new ArrayList();
									list.add(tbInfo.getDbid());
									list.add(tbInfo.getName());
									while (rs.next()) {
										list.add(rs.getLong("sum"));
										list.add(rs.getLong("csum"));
										list.add(rs.getTimestamp("cmintime"));
									}
									return list;
								}
							});
		} catch (Exception e) {
			logger.error(e);
			rs = new ArrayList();
			rs.add(tbInfo.getDbid());
			rs.add(tbInfo.getName());
			rs.add(-1L);
			rs.add(-1L);
			rs.add(null);
		}
		return rs;
	}

	@SuppressWarnings("rawtypes")
	private final class TBDataSumGetor implements Callable<List> {
		private TBInfo tbInfo;

		public TBDataSumGetor(TBInfo tbInfo) {
			super();
			this.tbInfo = tbInfo;
		}

		@Override
		public List call() throws Exception {
			return getTBDataSum(tbInfo);
		}
	}

	private void saveTBDataStatus(List<TBDataStatus> list) {
		final List<TBDataStatus> newStatus = new ArrayList<TBDataStatus>();
		for (TBDataStatus a : list) {
			TBDataStatus b = curTBDataStatus.get(a.getTbid());
			if (b == null || a.getSrcSum() != b.getSrcSum() || a.getDestSum() != b.getDestSum()
					|| a.getSrcNeedChangeSum() != b.getSrcNeedChangeSum()
					|| a.getSrcNeedDeleteSum() != b.getSrcNeedDeleteSum()
					|| (a.getSrcNeedChangeMinT() == null ? -1
							: a.getSrcNeedChangeMinT().getTime()) != (b.getSrcNeedChangeMinT() == null ? -1
									: b.getSrcNeedChangeMinT().getTime())
					|| (a.getSrcNeedDeleteMinT() == null ? -1
							: a.getSrcNeedDeleteMinT().getTime()) != (b.getSrcNeedDeleteMinT() == null ? -1
									: b.getSrcNeedDeleteMinT().getTime())) {
				newStatus.add(a);
				curTBDataStatus.put(a.getTbid(), a);
			}
		}

		if (newStatus.size() == 0) {
			return;
		}

		systore.jdbc.batchUpdate(
				"insert into tbdatastatus(id,tbid,ssum,sncsum,sncmintime,sndsum,sndmintime,dsum,ct) values(?,?,?,?,?,?,?,?,?)",
				new BatchPreparedStatementSetter() {
					@Override
					public void setValues(PreparedStatement ps, int i) throws SQLException {
						ps.setLong(1, newStatus.get(i).getId());
						ps.setLong(2, newStatus.get(i).getTbid());
						ps.setLong(3, newStatus.get(i).getSrcSum());
						ps.setLong(4, newStatus.get(i).getSrcNeedChangeSum());

						Date time = newStatus.get(i).getSrcNeedChangeMinT();
						ps.setTimestamp(5, time == null ? null : new java.sql.Timestamp(time.getTime()));
						ps.setLong(6, newStatus.get(i).getSrcNeedDeleteSum());
						time = newStatus.get(i).getSrcNeedDeleteMinT();
						ps.setTimestamp(7, time == null ? null : new java.sql.Timestamp(time.getTime()));
						ps.setLong(8, newStatus.get(i).getDestSum());
						ps.setTimestamp(9, new java.sql.Timestamp(newStatus.get(i).getCt().getTime()));
					}

					@Override
					public int getBatchSize() {
						return newStatus.size();
					}
				});

	}

	private void load_TBDataCache() {
		curTBDataStatus.clear();
		List<TBDataStatus> tbDataStatuses = systore.jdbc.query(
				"select id,tbid,ssum,sncsum,sncmintime,sndsum,sndmintime,dsum,ct from tbdatastatus ",
				new TBDataStatusMapper());
		for (TBDataStatus tbDataStatus : tbDataStatuses) {
			if (tbDataStatus == null) {
				continue;
			}
			curTBDataStatus.put(tbDataStatus.getTbid(), tbDataStatus);
		}
	}

	private final class TBDataStatusMapper implements RowMapper<TBDataStatus> {
		public TBDataStatus mapRow(ResultSet rs, int rowNum) {
			TBDataStatus tbDataStatus = null;
			try {

				tbDataStatus = new TBDataStatus(rs.getLong("id"), rs.getLong("tbid"), rs.getLong("ssum"),
						rs.getLong("sncsum"), rs.getTimestamp("sncmintime"), rs.getLong("sndsum"),
						rs.getTimestamp("sndmintime"), rs.getLong("dsum"), rs.getTimestamp("ct"));

			} catch (SQLException e) {
				logger.error(e.getMessage(), e);
			}
			return tbDataStatus;
		}
	}

}
