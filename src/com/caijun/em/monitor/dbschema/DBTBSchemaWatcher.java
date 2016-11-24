package com.caijun.em.monitor.dbschema;

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
import com.caijun.em.monitor.dbschema.TBMetaAnalyzer.TBMetaAnalysisRS;

public class DBTBSchemaWatcher {
	private static final String TIME_INTERVAL = "DBSchema.TBDetect.TimeInterval";
	private static final String TB_TYPE = "DBSchema.tb";
	private Logger logger = Logger.getLogger("dbschema");
	private Lock lock;

	private Systore systore;
	private TBMetaDetector tbMetaDetector;
	private TBMetaAnalyzer tbMetaAnalyzer;

	private ScheduledExecutorService scheduler;
	private long time_interval;
	private boolean isStart;

	public DBTBSchemaWatcher(Systore systore, TBMetaDetector tbMetaDetector,
			TBMetaAnalyzer tbMetaAnalyzer) {
		super();
		isStart = false;
		lock = new ReentrantLock();
		this.systore = systore;
		time_interval = systore.props.getInMinLimit(TIME_INTERVAL, 240L, 1L);
		this.tbMetaDetector = tbMetaDetector;
		this.tbMetaAnalyzer = tbMetaAnalyzer;

	}

	public void start() {
		lock.tryLock();
		logger.info("启动数据库表结构监控...");
		try {
			if (isStart) {
				logger.info("数据库表结构监控已经启动,不能重复启动.");
				return;
			}
			isStart = true;
			scheduler = Executors.newSingleThreadScheduledExecutor();
			scheduler.schedule(new DoWork(), 0L, TimeUnit.SECONDS);
			logger.info("启动数据库表结构监控成功.");
		} finally {
			lock.unlock();
		}
	}

	public void stop() {
		lock.tryLock();
		logger.info("停止数据库表结构监控...");
		try {
			if (isStart) {
				scheduler.shutdown();
				isStart = false;
				logger.info("停止数据库表结构监控成功.");
			} else {
				logger.info("数据库表结构监控已经停止,不能重复停止.");
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
				logger.info("数据库表结构监控[" + counter + "]开始...");
				logger.info("数据库表结构监控[" + counter + "]采样开始...");
				long begin = new Date().getTime();
				tbMetaDetector.sampling();
				long end = new Date().getTime();
				double time = (end - begin) / 1000.0;
				logger.info("数据库表结构监控[" + counter + "]采样结束,用时:" + time + "s");
				logger.info("数据库表结构监控[" + counter + "]分析开始...");
				begin = new Date().getTime();
				List<TBMetaAnalysisRS> tbMetaRSList = tbMetaAnalyzer
						.doing(systore.dbSchema.getAllCurTBStatus());
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("数据库表结构监控[" + counter + "]分析结束,用时:" + time + "s");

				if (logger.isDebugEnabled()) {
					logger.debug("发生变化的表:");
					for (TBMetaAnalysisRS tbMetaRS : tbMetaRSList) {
						logger.debug(tbMetaRS.getName());
					}
				}

				logger.info("数据库表结构监控[" + counter + "]开始发送异常消息..");
				begin = new Date().getTime();
				for (TBMetaAnalysisRS tbMetaRS : tbMetaRSList) {
					MSender.sendMsg(TB_TYPE, tbMetaRS.toXML());
				}
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("数据库表结构监控[" + counter + "]发送异常消息结束,用时" + time + "s");
				logger.info("数据库表结构监控[" + counter + "]结束.");
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

}
