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
import com.caijun.em.monitor.dbschema.TrigMetaAnalyzer.TrigMetaAnalysisRS;

public class DBTrigSchemaWatcher {
	private static final String TIME_INTERVAL = "DBSchema.TrigDetect.TimeInterval";
	private static final String STRUCT_INTERVAL = "DBSchema.TrigDetect.structInterval";
	private static final String TRIG_TYPE = "DBSchema.trig";
	private Logger logger = Logger.getLogger("dbschema");
	private Lock lock;

	private Systore systore;
	private TrigMetaDetector trigMetaDetector;
	private TrigMetaAnalyzer trigMetaAnalyzer;

	private ScheduledExecutorService scheduler;
	private long time_interval;
	private boolean isStart;

	public DBTrigSchemaWatcher(Systore systore,
			TrigMetaDetector trigMetaDetector, TrigMetaAnalyzer trigMetaAnalyzer) {
		super();
		isStart = false;
		lock = new ReentrantLock();
		this.systore = systore;
		this.trigMetaDetector = trigMetaDetector;
		this.trigMetaAnalyzer = trigMetaAnalyzer;
		time_interval = systore.props.getInMinLimit(TIME_INTERVAL, 5L, 1L);
	}

	public void start() {
		lock.tryLock();
		logger.info("启动数据库触发器结构监控...");
		try {
			if (isStart) {
				logger.info("数据库触发器结构监控已经启动,不能重复启动.");
				return;
			}
			isStart = true;
			scheduler = Executors.newSingleThreadScheduledExecutor();
			scheduler.schedule(new DoWork(), 0L, TimeUnit.SECONDS);
			logger.info("启动数据库触发器结构监控成功.");
		} finally {
			lock.unlock();
		}
	}

	public void stop() {
		lock.tryLock();
		logger.info("停止数据库触发器结构监控...");
		try {
			if (isStart) {
				scheduler.shutdown();
				isStart = false;
				logger.info("停止数据库触发器结构监控成功.");
			} else {
				logger.info("数据库触发器结构监控已经停止,不能重复停止.");
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
			boolean withStruct = true;
			try {
				long interval = systore.props.getInMinLimit(STRUCT_INTERVAL,
						1L, 0L) + 1;
				if (interval != 0 && counter % (interval + 1) != 1) {
					withStruct = false;
				}
				logger.info("数据库触发器结构监控[" + counter + "]开始...");
				logger.info("数据库触发器结构监控[" + counter + "]采样开始...");
				long begin = new Date().getTime();
				trigMetaDetector.sampling(withStruct);
				long end = new Date().getTime();
				double time = (end - begin) / 1000.0;
				logger.info("数据库触发器结构监控[" + counter + "]采样结束,用时:" + time + "s");
				logger.info("数据库触发器结构监控[" + counter + "]分析开始...");
				begin = new Date().getTime();
				List<TrigMetaAnalysisRS> trigMetaRSList = trigMetaAnalyzer
						.doing(systore.dbSchema.getAllCurTrigStatus(),
								withStruct);
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("数据库触发器结构监控[" + counter + "]分析结束,用时:" + time + "s");

				if (logger.isDebugEnabled()) {
					logger.debug("数据库触发器结构监控[" + counter + "]发生变化的触发器:");
					for (TrigMetaAnalysisRS trigMetaRS : trigMetaRSList) {
						logger.debug(trigMetaRS.getName());
					}
				}

				logger.info("数据库触发器结构监控[" + counter + "]开始发送异常消息..");
				begin = new Date().getTime();
				for (TrigMetaAnalysisRS trigMetaRS : trigMetaRSList) {
					MSender.sendMsg(TRIG_TYPE, trigMetaRS.toXML());
				}
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("数据库触发器结构监控[" + counter + "]发送异常消息结束,用时" + time
						+ "s");

				logger.info("数据库触发器结构监控[" + counter + "]结束.");
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
