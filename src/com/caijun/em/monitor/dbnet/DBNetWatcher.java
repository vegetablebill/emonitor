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
		logger.info("�������ݿ�������...");
		try {
			if (isStart) {
				logger.info("���ݿ��������Ѿ�����,�����ظ�����.");
				return;
			}
			isStart = true;
			scheduler = Executors.newSingleThreadScheduledExecutor();
			scheduler.schedule(new DoWork(), 0L, TimeUnit.SECONDS);
			logger.info("�������ݿ������سɹ�.");
		} finally {
			lock.unlock();
		}
	}

	public void stop() {
		lock.tryLock();
		logger.info("ֹͣ���ݿ�������...");
		try {
			if (isStart) {
				scheduler.shutdown();
				isStart = false;
				logger.info("ֹͣ���ݿ������سɹ�.");
			} else {
				logger.info("���ݿ��������Ѿ�ֹͣ,�����ظ�ֹͣ.");
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
				logger.info("���ݿ����Ӽ��[" + counter + "]��ʼ...");

				logger.info("������ʼ...");
				long begin = new Date().getTime();
				dbNetDetector.sampling();
				long end = new Date().getTime();
				double time = (end - begin) / 1000.0;
				logger.info("��������,��ʱ:" + time + "s");

				logger.info("�����쳣������ʼ...");
				begin = new Date().getTime();
				List<DBDisConnAnalysisRS> disconnList = dbDisConnAnalyzer
						.doing(systore.dbNet.getAllCurDBStatus());
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("�����쳣��������,��ʱ:" + time + "s");

				logger.info("���Ӷ���������ʼ...");
				begin = new Date().getTime();
				List<DBShakeAnalysisRS> shakeList = dbConnShakeAnalyzer.doing();
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("���Ӷ�����������,��ʱ:" + time + "s");

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
					logger.debug("�����쳣�����ݿ�:");
					logger.debug(String.format("%-6s%-6s%-6s", "ID", "AID",
							"url"));
					for (DBDisConnAnalysisRS disconn : disconnList) {
						DBInfo dbinfo = systore.dbNet.getDBInfo(disconn
								.getDbId());
						logger.debug(String.format("%-6d%-6d%-6s",
								dbinfo.getId(), dbinfo.getAid(),
								dbinfo.getUrl()));
					}
					logger.debug("���ӷ������������ݿ�:");
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
				logger.info("��ʼ�����쳣��Ϣ..");
				begin = new Date().getTime();
				for (DBDisConnAnalysisRS disconn : disconnList) {
					MSender.sendMsg(DISCONN_TYPE, disconn.toXML());
				}
				for (DBShakeAnalysisRS shake : shakeList) {
					MSender.sendMsg(SHAKE_TYPE, shake.toXML());
				}
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("�����쳣��Ϣ����,��ʱ" + time + "s");
				logger.info("���ݿ����Ӽ��[" + counter + "]����.");
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
