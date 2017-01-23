package com.caijun.em.monitor.dbnet;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.caijun.em.Systore;
import com.caijun.em.mail.MSender;
import com.caijun.em.monitor.dbnet.DBConnShakeAnalyzer.DBShakeAnalysisRS;
import com.caijun.em.monitor.dbnet.DBDisConnAnalyzer.DBDisConnAnalysisRS;

public class DBNetWatcher {
	private static final String TIME_INTERVAL = "DBNet.ConnDetect.TimeInterval";
	private static final String DISCONN_TYPE = "dbnet.disconn";
	private static final String SHAKE_TYPE = "dbnet.shake";
	private Logger logger = Logger.getLogger("dbnet");
	private Lock lock;
	private Systore systore;
	private DBNetDetector dbNetDetector;
	private DBDisConnAnalyzer dbDisConnAnalyzer;
	private DBConnShakeAnalyzer dbConnShakeAnalyzer;
	private ScheduledExecutorService scheduler;
	private long time_interval;
	private boolean isStart;

	public DBNetWatcher(Systore systore, DBNetDetector dbNetDetector,
			DBDisConnAnalyzer dbDisConnAnalyzer,
			DBConnShakeAnalyzer dbConnShakeAnalyzer) {
		super();
		isStart = false;
		lock = new ReentrantLock();
		this.systore = systore;
		this.dbNetDetector = dbNetDetector;
		this.dbDisConnAnalyzer = dbDisConnAnalyzer;
		this.dbConnShakeAnalyzer = dbConnShakeAnalyzer;
		time_interval = systore.props.getInMinLimit(TIME_INTERVAL, 5L, 1L);
	}

	public void start() {
		lock.tryLock();
		logger.info("启动数据库网络监控...");
		try {
			if (isStart) {
				logger.info("数据库网络监控已经启动,不能重复启动.");
				return;
			}
			isStart = true;
			scheduler = Executors.newSingleThreadScheduledExecutor();
			scheduler.schedule(new DoWork(), 0L, TimeUnit.SECONDS);
			logger.info("启动数据库网络监控成功.");
		} finally {
			lock.unlock();
		}
	}

	public void stop() {
		lock.tryLock();
		logger.info("停止数据库网络监控...");
		try {
			if (isStart) {
				scheduler.shutdown();
				isStart = false;
				logger.info("停止数据库网络监控成功.");
			} else {
				logger.info("数据库网络监控已经停止,不能重复停止.");
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
				logger.info("数据库连接监控[" + counter + "]开始...");

				logger.info("采样开始...");
				long begin = new Date().getTime();
				dbNetDetector.sampling();
				long end = new Date().getTime();
				double time = (end - begin) / 1000.0;
				logger.info("采样结束,用时:" + time + "s");

				logger.info("连接异常分析开始...");
				begin = new Date().getTime();
				List<DBDisConnAnalysisRS> disconnList = dbDisConnAnalyzer
						.doing(systore.dbNet.getAllCurDBStatus());
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("连接异常分析结束,用时:" + time + "s");

				logger.info("连接抖动分析开始...");
				begin = new Date().getTime();
				List<DBShakeAnalysisRS> shakeList = dbConnShakeAnalyzer.doing();
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("连接抖动分析结束,用时:" + time + "s");

				for (DBDisConnAnalysisRS disconn : disconnList) {
					for (int i = 0; i < shakeList.size(); i++) {
						DBShakeAnalysisRS shake = shakeList.get(i);
						if (disconn.getDbId() == shake.getDbId()) {
							shakeList.remove(i);
							break;
						}
					}
				}

				if (logger.isDebugEnabled()) {
					logger.debug("连接异常的数据库:");
					logger.debug(String.format("%-6s%-6s%-6s", "ID", "AID",
							"url"));
					for (DBDisConnAnalysisRS disconn : disconnList) {
						DBInfo dbinfo = systore.dbNet.getDBInfo(disconn
								.getDbId());
						logger.debug(String.format("%-6d%-6d%-6s",
								dbinfo.getId(), dbinfo.getAid(),
								dbinfo.getUrl()));
					}
					logger.debug("连接发生抖动的数据库:");
					logger.debug(String.format("%-6s%-6s%-6s", "ID", "AID",
							"url"));
					for (DBShakeAnalysisRS shake : shakeList) {
						DBInfo dbinfo = systore.dbNet
								.getDBInfo(shake.getDbId());
						logger.debug(String.format("%-6d%-6d%-6s",
								dbinfo.getId(), dbinfo.getAid(),
								dbinfo.getUrl()));
					}
				}
				logger.info("开始发送异常消息..");
				begin = new Date().getTime();
				for (DBDisConnAnalysisRS disconn : disconnList) {
					MSender.sendMsg(DISCONN_TYPE, disconn.toXML());
				}
				for (DBShakeAnalysisRS shake : shakeList) {
					MSender.sendMsg(SHAKE_TYPE, shake.toXML());
				}
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("发送异常消息结束,用时" + time + "s");
				logger.info("数据库连接监控[" + counter + "]结束.");
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}

			counter++;
			Long interval = 0l;
			try {
				interval = systore.props.getInMinLimit(TIME_INTERVAL, 5L, 1L);
			} catch (Exception e) {
				interval = time_interval;
			}
			scheduler.schedule(this, interval, TimeUnit.MINUTES);
		}
	}

}
