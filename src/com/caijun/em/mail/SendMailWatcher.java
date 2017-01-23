package com.caijun.em.mail;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.caijun.em.Systore;
import com.caijun.mail.Email;
import com.caijun.mail.SendMailSession;
import com.caijun.mail.SmtpServer;

public class SendMailWatcher {
	private static final String TIME_INTERVAL = "Mail.Send.TimeInterval";
	private static final String MAIL_SERVER_IP = "Mail.Server.ip";
	private static final String MAIL_USER = "Mail.User";
	private static final String MAIL_PASSWORD = "Mail.Password";
	private Logger logger = Logger.getLogger("mail");
	private Lock lock;
	private ScheduledExecutorService scheduler;
	private long time_interval;
	private boolean isStart;
	private Systore systore;
	private List<SendMail> senders = null;

	public SendMailWatcher(Systore systore) {
		isStart = false;
		lock = new ReentrantLock();
		this.systore = systore;
		time_interval = systore.props.getInMinLimit(TIME_INTERVAL, 1L, 1L);
		senders = new ArrayList<SendMail>();
		senders.add(new DBNetSendMail(systore, logger));
		senders.add(new DBSchemaTrigSendMail(systore, logger));
		senders.add(new DBSchemaTBSendMail(systore, logger));
		senders.add(new TBDataSendMail(systore, logger));
	}

	public void start() {
		lock.tryLock();
		logger.info("启动邮件发送服务...");
		try {
			if (isStart) {
				logger.info("邮件发送服务已经启动,不能重复启动.");
				return;
			}
			isStart = true;
			scheduler = Executors.newSingleThreadScheduledExecutor();
			scheduler.schedule(new DoWork(), 0L, TimeUnit.SECONDS);
			logger.info("启邮件发送服务成功.");
		} finally {
			lock.unlock();
		}
	}

	public void stop() {
		lock.tryLock();
		logger.info("停止邮件发送服务...");
		try {
			if (isStart) {
				scheduler.shutdown();
				isStart = false;
				logger.info("停止邮件发送服务成功.");
			} else {
				logger.info("邮件发送服务已经停止,不能重复停止.");
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

			long begin = 0;
			long end = 0;
			double time = 0.0;

			try {
				logger.info("邮件发送服务[" + counter + "]开始...");
				begin = new Date().getTime();

				String ip = systore.props.get(MAIL_SERVER_IP, "localhost");
				String user = systore.props.get(MAIL_USER, "monit@monit.com");
				String password = systore.props.get(MAIL_PASSWORD, "123456");
				for (SendMail sender : senders) {
					sender.beforeSend();
				}
				SmtpServer smtpServer = SmtpServer.create(ip).authenticateWith(user, password);
				SendMailSession session = smtpServer.createSession();
				session.open();
				for (SendMail sender : senders) {
					Email email = sender.send();
					if (email == null) {
						sender.afterSend(null);
					} else {
						sender.afterSend(session.sendMail(email));
					}
				}
				session.close();

				end = new Date().getTime();
				time = (end - begin) / 1000.0;
				logger.info("邮件发送服务[" + counter + "]结束,用时" + time + "s");

			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}

			counter++;
			Long interval = 0l;
			try {
				interval = systore.props.getInMinLimit(TIME_INTERVAL, 1L, 1L);
			} catch (Exception e) {
				interval = time_interval;
			}
			scheduler.schedule(this, interval, TimeUnit.MINUTES);
		}
	}

}
