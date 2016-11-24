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
		logger.info("�������ݿ��ṹ���...");
		try {
			if (isStart) {
				logger.info("���ݿ��ṹ����Ѿ�����,�����ظ�����.");
				return;
			}
			isStart = true;
			scheduler = Executors.newSingleThreadScheduledExecutor();
			scheduler.schedule(new DoWork(), 0L, TimeUnit.SECONDS);
			logger.info("�������ݿ��ṹ��سɹ�.");
		} finally {
			lock.unlock();
		}
	}

	public void stop() {
		lock.tryLock();
		logger.info("ֹͣ���ݿ��ṹ���...");
		try {
			if (isStart) {
				scheduler.shutdown();
				isStart = false;
				logger.info("ֹͣ���ݿ��ṹ��سɹ�.");
			} else {
				logger.info("���ݿ��ṹ����Ѿ�ֹͣ,�����ظ�ֹͣ.");
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
				logger.info("���ݿ��ṹ���[" + counter + "]��ʼ...");
				logger.info("���ݿ��ṹ���[" + counter + "]������ʼ...");
				long begin = new Date().getTime();
				tbMetaDetector.sampling();
				long end = new Date().getTime();
				double time = (end - begin) / 1000.0;
				logger.info("���ݿ��ṹ���[" + counter + "]��������,��ʱ:" + time + "s");
				logger.info("���ݿ��ṹ���[" + counter + "]������ʼ...");
				begin = new Date().getTime();
				List<TBMetaAnalysisRS> tbMetaRSList = tbMetaAnalyzer
						.doing(systore.dbSchema.getAllCurTBStatus());
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("���ݿ��ṹ���[" + counter + "]��������,��ʱ:" + time + "s");

				if (logger.isDebugEnabled()) {
					logger.debug("�����仯�ı�:");
					for (TBMetaAnalysisRS tbMetaRS : tbMetaRSList) {
						logger.debug(tbMetaRS.getName());
					}
				}

				logger.info("���ݿ��ṹ���[" + counter + "]��ʼ�����쳣��Ϣ..");
				begin = new Date().getTime();
				for (TBMetaAnalysisRS tbMetaRS : tbMetaRSList) {
					MSender.sendMsg(TB_TYPE, tbMetaRS.toXML());
				}
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("���ݿ��ṹ���[" + counter + "]�����쳣��Ϣ����,��ʱ" + time + "s");
				logger.info("���ݿ��ṹ���[" + counter + "]����.");
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
