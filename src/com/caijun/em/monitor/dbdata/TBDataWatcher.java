package com.caijun.em.monitor.dbdata;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.caijun.em.Area;
import com.caijun.em.Systore;
import com.caijun.em.mail.MSender;
import com.caijun.em.monitor.dbnet.DBInfo;
import com.caijun.em.monitor.dbschema.TBInfo;
import com.caijun.utils.str.StringUtil;

public class TBDataWatcher {
	private static final String TIME_INTERVAL = "TBData.TBDataDetect.TimeInterval";
	private static final String TBDATA_TYPE = "tbdata";
	private Logger logger = Logger.getLogger("tbdata");
	private Lock lock;

	private Systore systore;
	private TBDataDetector tbDataDetector;
	private TBDataAnalyzer tbDataAnalyzer;

	private ScheduledExecutorService scheduler;
	private long time_interval;
	private boolean isStart;

	public TBDataWatcher(Systore systore, TBDataDetector tbDataDetector, TBDataAnalyzer tbDataAnalyzer) {
		super();
		isStart = false;
		lock = new ReentrantLock();
		this.systore = systore;
		time_interval = systore.props.getInMinLimit(TIME_INTERVAL, 240L, 1L);
		this.tbDataDetector = tbDataDetector;
		this.tbDataAnalyzer = tbDataAnalyzer;

	}

	public void start() {
		lock.tryLock();
		logger.info("启动数据库表数据监控...");
		try {
			if (isStart) {
				logger.info("数据库表数据监控已经启动,不能重复启动.");
				return;
			}
			isStart = true;
			scheduler = Executors.newSingleThreadScheduledExecutor();
			scheduler.schedule(new DoWork(), 0L, TimeUnit.SECONDS);
			logger.info("启动数据库表数据监控成功.");
		} finally {
			lock.unlock();
		}
	}

	public void stop() {
		lock.tryLock();
		logger.info("停止数据库表数据监控...");
		try {
			if (isStart) {
				scheduler.shutdown();
				isStart = false;
				logger.info("停止数据库表数据监控成功.");
			} else {
				logger.info("数据库表数据监控已经停止,不能重复停止.");
			}

		} finally {
			lock.unlock();
		}
	}

	private final class DoWork implements Runnable {
		private long counter = 1;

		@Override
		public void run() {
			if (!isStart) {
				return;
			}
			try {
				logger.info("数据库表数据监控[" + counter + "]开始...");
				logger.info("数据库表数据监控[" + counter + "]采样开始...");
				long begin = new Date().getTime();
				tbDataDetector.sampling();
				long end = new Date().getTime();
				double time = (end - begin) / 1000.0;
				logger.info("数据库表数据监控[" + counter + "]采样结束,用时:" + time + "s");
				logger.info("数据库表数据监控[" + counter + "]分析开始...");
				begin = new Date().getTime();
				List<TBDataStatus> tbDataStatusList = tbDataAnalyzer.doing();
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("数据库表数据监控[" + counter + "]分析结束,用时:" + time + "s");

				if (logger.isDebugEnabled()) {
					logger.debug("数据交换异常的表:");
					for (TBDataStatus status : tbDataStatusList) {
						TBInfo tbInfo = systore.dbSchema.getTBInfo(status.getTbid());
						logger.debug(tbInfo.getName());
					}
				}

				logger.info("数据库表数据监控[" + counter + "]开始发送异常消息..");

				begin = new Date().getTime();
				for (TBDataStatus status : tbDataStatusList) {
					if (status.getSrcSum() == -1 || status.getDestSum() == -1 || status.getSrcNeedChangeSum() == -1
							|| status.getSrcNeedDeleteSum() == -1) {
						continue;
					}
					MSender.sendMsg(TBDATA_TYPE, createMSG(status));
				}
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("数据库表数据监控[" + counter + "]发送异常消息结束,用时" + time + "s");
				logger.info("数据库表数据监控[" + counter + "]结束.");
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}

			counter++;
			Long interval = 0l;
			try {
				interval = systore.props.getInMinLimit(TIME_INTERVAL, 240L, 1L);
			} catch (Exception e) {
				interval = time_interval;
			}
			scheduler.schedule(this, interval, TimeUnit.MINUTES);

		}
	}

	private String createMSG(TBDataStatus status) {
		StringBuffer sb = new StringBuffer("<m>");
		TBInfo tbInfo = systore.dbSchema.getTBInfo(status.getTbid());
		DBInfo dbInfo = systore.dbNet.getDBInfo(tbInfo.getDbid());
		Area area = systore.getArea(dbInfo.getAid());
		sb.append("<an>").append(area.getName()).append("</an>");
		sb.append("<dn>").append(dbInfo.getUrl().replaceFirst(".*@", "").replaceAll(":.*", "")).append("</dn>");
		sb.append("<tn>").append(tbInfo.getName()).append("</tn>");
		sb.append("<srcn>").append(status.getSrcSum()).append("</srcn>");
		sb.append("<destn>").append(status.getDestSum()).append("</destn>");
		sb.append("<srcncn>").append(status.getSrcNeedChangeSum()).append("</srcncn>");
		sb.append("<srcndn>").append(status.getSrcNeedDeleteSum()).append("</srcndn>");
		sb.append("<srcnct>").append(status.getSrcNeedChangeMinT() == null ? "无"
				: StringUtil.formatDate("yyyy-MM-dd HH:mm", status.getSrcNeedChangeMinT())).append("</srcnct>");
		sb.append("<srcndt>").append(status.getSrcNeedDeleteMinT() == null ? "无"
				: StringUtil.formatDate("yyyy-MM-dd HH:mm", status.getSrcNeedDeleteMinT())).append("</srcndt>");
		sb.append("<ct>").append(StringUtil.formatDate("yyyy-MM-dd HH:mm", status.getCt())).append("</ct>");
		sb.append("</m>");
		return sb.toString();
	}

}
