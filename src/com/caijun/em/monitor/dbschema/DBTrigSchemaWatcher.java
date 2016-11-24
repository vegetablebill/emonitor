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
		logger.info("�������ݿⴥ�����ṹ���...");
		try {
			if (isStart) {
				logger.info("���ݿⴥ�����ṹ����Ѿ�����,�����ظ�����.");
				return;
			}
			isStart = true;
			scheduler = Executors.newSingleThreadScheduledExecutor();
			scheduler.schedule(new DoWork(), 0L, TimeUnit.SECONDS);
			logger.info("�������ݿⴥ�����ṹ��سɹ�.");
		} finally {
			lock.unlock();
		}
	}

	public void stop() {
		lock.tryLock();
		logger.info("ֹͣ���ݿⴥ�����ṹ���...");
		try {
			if (isStart) {
				scheduler.shutdown();
				isStart = false;
				logger.info("ֹͣ���ݿⴥ�����ṹ��سɹ�.");
			} else {
				logger.info("���ݿⴥ�����ṹ����Ѿ�ֹͣ,�����ظ�ֹͣ.");
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
				logger.info("���ݿⴥ�����ṹ���[" + counter + "]��ʼ...");
				logger.info("���ݿⴥ�����ṹ���[" + counter + "]������ʼ...");
				long begin = new Date().getTime();
				trigMetaDetector.sampling(withStruct);
				long end = new Date().getTime();
				double time = (end - begin) / 1000.0;
				logger.info("���ݿⴥ�����ṹ���[" + counter + "]��������,��ʱ:" + time + "s");
				logger.info("���ݿⴥ�����ṹ���[" + counter + "]������ʼ...");
				begin = new Date().getTime();
				List<TrigMetaAnalysisRS> trigMetaRSList = trigMetaAnalyzer
						.doing(systore.dbSchema.getAllCurTrigStatus(),
								withStruct);
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("���ݿⴥ�����ṹ���[" + counter + "]��������,��ʱ:" + time + "s");

				if (logger.isDebugEnabled()) {
					logger.debug("���ݿⴥ�����ṹ���[" + counter + "]�����仯�Ĵ�����:");
					for (TrigMetaAnalysisRS trigMetaRS : trigMetaRSList) {
						logger.debug(trigMetaRS.getName());
					}
				}

				logger.info("���ݿⴥ�����ṹ���[" + counter + "]��ʼ�����쳣��Ϣ..");
				begin = new Date().getTime();
				for (TrigMetaAnalysisRS trigMetaRS : trigMetaRSList) {
					MSender.sendMsg(TRIG_TYPE, trigMetaRS.toXML());
				}
				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("���ݿⴥ�����ṹ���[" + counter + "]�����쳣��Ϣ����,��ʱ" + time
						+ "s");

				logger.info("���ݿⴥ�����ṹ���[" + counter + "]����.");
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
