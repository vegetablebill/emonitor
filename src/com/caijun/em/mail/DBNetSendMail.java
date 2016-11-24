package com.caijun.em.mail;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.caijun.em.Area;
import com.caijun.em.Systore;
import com.caijun.em.monitor.dbnet.DBConnShakeAnalyzer.DBShakeAnalysisRS;
import com.caijun.em.monitor.dbnet.DBDisConnAnalyzer.DBDisConnAnalysisRS;
import com.caijun.em.monitor.dbnet.DBInfo;
import com.caijun.mail.Email;
import com.caijun.utils.str.StringUtil;

public class DBNetSendMail extends SendMail {
	private static String MAIL_TYPE_DISCONN = "dbnet.disconn";
	private static String MAIL_TYPE_SHAKE = "dbnet.shake";
	private static String MAIL_TO = "Mail.DBNet.ToUser";
	private static String MAIL_REPEAT_INTERVAL = "Mail.DBNet.Repeat_Interval";
	private Map<String, Msg> disconn_LSMS;
	private Map<String, Msg> shake_LSMS;
	private List<Msg> msgs;
	private String mailContent;

	public DBNetSendMail(Systore systore, Logger logger) {
		super(systore, logger);
		disconn_LSMS = new HashMap<String, Msg>();
		shake_LSMS = new HashMap<String, Msg>();
		Date begin = new Date(new Date().getTime()
				- systore.props.getInMinLimit(MAIL_REPEAT_INTERVAL, 180L, 1L)
				* 60000);
		List<Msg> msgs = this.getLatestSendMS(MAIL_TYPE_DISCONN, begin);
		for (Msg msg : msgs) {
			disconn_LSMS.put(msg.digest, msg);
		}
		msgs = this.getLatestSendMS(MAIL_TYPE_SHAKE, begin);
		for (Msg msg : msgs) {
			shake_LSMS.put(msg.digest, msg);
		}

	}

	@Override
	public void beforeSend() {
		if (logger != null) {
			logger.info("�ϴδ�������ݿ������ж���Ϣ"
					+ disconn_LSMS.values().size()
					+ "��"
					+ (!logger.isDebugEnabled() ? "." : ",��ϢIDΪ:"
							+ serializeMsgByID(disconn_LSMS.values())));
			logger.info("�ϴδ�������ݿ����Ӷ�����Ϣ"
					+ shake_LSMS.values().size()
					+ "��"
					+ (!logger.isDebugEnabled() ? "." : ",��ϢIDΪ:"
							+ serializeMsgByID(shake_LSMS.values())));
		}
		msgs = getNeedSendMSG();
		if (logger != null) {
			logger.info("��Ҫ�����ʼ������ݿ������쳣��Ϣ"
					+ msgs.size()
					+ "��"
					+ (!logger.isDebugEnabled() ? "." : ",��ϢIDΪ:"
							+ serializeMsgByID(msgs)));
		}

		if (msgs == null || msgs.size() == 0) {
			return;
		}
		mailContent = creatMailContent(msgs);
	}

	@Override
	public Email send() {
		if (msgs == null || msgs.size() == 0) {
			return null;
		}
		String user = systore.props.get(MAIL_USER);
		String to = systore.props.get(MAIL_TO);
		Email email = Email.create().from(user).to(to);
		email.subject("���ݿ������쳣����");
		email.addHtml(mailContent);
		return email;

	}

	@Override
	public void afterSend(String emailID) {
		if (msgs.size() == 0 && logger != null) {
			logger.info("û�з������ݿ������쳣�ʼ�.");
		} else if (logger != null) {
			logger.info("�������ݿ������쳣�ʼ��ɹ�,�ʼ����[" + emailID + "]");
		}
		setSended(msgs);
		long timeFlag = new Date().getTime()
				- systore.props.getInMinLimit(MAIL_REPEAT_INTERVAL, 180L, 1L)
				* 60000;
		for (Msg msg : disconn_LSMS.values()) {
			if (timeFlag > msg.ct.getTime()) {
				disconn_LSMS.remove(msg.digest);
			}
		}
		for (Msg msg : shake_LSMS.values()) {
			if (timeFlag > msg.ct.getTime()) {
				shake_LSMS.remove(msg.digest);
			}
		}
		for (Msg msg : msgs) {
			if (MAIL_TYPE_DISCONN.equals(msg.type)) {
				disconn_LSMS.put(msg.digest, msg);
			} else if (MAIL_TYPE_SHAKE.equals(msg.type)) {
				shake_LSMS.put(msg.digest, msg);
			}
		}

	}

	private List<Msg> getNeedSendMSG() {
		boolean sendFlag = false;
		List<Msg> rs = new ArrayList<Msg>();
		List<Msg> skipMsgs = this.getUndealMS(MAIL_TYPE_DISCONN);
		skipMsgs.addAll(this.getUndealMS(MAIL_TYPE_SHAKE));
		logger.info("������Ҫ��������ݿ������ж���Ϣ"
				+ skipMsgs.size()
				+ "��"
				+ (!logger.isDebugEnabled() ? "." : ",��ϢIDΪ:"
						+ serializeMsgByID(skipMsgs)));
		long time = systore.props.getInMinLimit(MAIL_REPEAT_INTERVAL, 180L, 1L) * 60000;
		Map<String, List<Msg>> msgs_map = this.groupByDigest(skipMsgs);
		for (String key : msgs_map.keySet()) {
			Msg msg = this.getNewestOne(msgs_map.get(key));
			rs.add(msg);
			if ((MAIL_TYPE_DISCONN.equals(msg.type) && (!disconn_LSMS
					.containsKey(key) || msg.ct.getTime()
					- disconn_LSMS.get(key).ct.getTime() >= time))
					|| (MAIL_TYPE_SHAKE.equals(msg.type) && (!shake_LSMS
							.containsKey(key) || msg.ct.getTime()
							- shake_LSMS.get(key).ct.getTime() >= time))) {
				sendFlag = true;
				skipMsgs.remove(msg);
			}
		}
		if (!sendFlag) {
			rs.clear();
		}
		this.skipMsgs(skipMsgs);
		return rs;
	}

	private String creatMailContent(List<Msg> msgs) {
		StringBuffer sb = new StringBuffer();
		List<Area> areas = systore.getAreas();
		List<DBInfo> dbinfos = systore.dbNet.getDBInfos();
		Map<Long, String> msgs_html = new HashMap<Long, String>();
		for (DBInfo dbInfo : dbinfos) {
			List<Msg> skips = new ArrayList<Msg>();
			for (Msg msg : msgs) {
				DBDisConnAnalysisRS disconn = null;
				DBShakeAnalysisRS shake = null;
				try {
					if (MAIL_TYPE_DISCONN.equals(msg.type)) {
						disconn = new DBDisConnAnalysisRS(msg.content);
					}
					if (MAIL_TYPE_SHAKE.equals(msg.type)) {
						shake = new DBShakeAnalysisRS(msg.content);
					}

				} catch (Exception e) {
					logger.error("��Ϣ[" + msg.id + "]�����쳣,����", e);
					skips.add(msg);
					continue;
				}

				if ((disconn != null && dbInfo.getId() == disconn.getDbId())
						|| (shake != null && dbInfo.getId() == shake.getDbId())) {
					StringBuffer tsb = new StringBuffer();
					tsb.append("<div><span class='font12'>���ݿ�&nbsp;&nbsp;</span><span class='font20'><b>[");
					tsb.append(dbInfo.getUrl().replaceFirst(".*@", "")
							.replaceAll(":.*", ""));
					tsb.append("]</b></span><span class='font12'>&nbsp;��&nbsp;&nbsp;</span><span class='font20'><b>");
					if (disconn.getCt() != null) {
						tsb.append(StringUtil.formatDate("yyyy-MM-dd HH:mm",
								disconn.getCt()));
					} else {
						tsb.append(StringUtil.formatDate("yyyy-MM-dd HH:mm",
								shake.getCt()));
					}
					tsb.append("</b></span><span class='font12'>&nbsp;��ʼ&nbsp;&nbsp;</span><span class='font20 red'><b>");
					if (MAIL_TYPE_DISCONN.equals(msg.type)) {
						tsb.append("�����ж�");
					} else if (MAIL_TYPE_SHAKE.equals(msg.type)
							&& !msgs_html.containsKey(dbInfo.getId())) {
						tsb.append("����ʱ��ʱ��");
					}
					tsb.append("</b></span></div>");
					msgs_html.put(dbInfo.getId(), tsb.toString());
					break;
				}
			}

			this.skipMsgs(skips);
			msgs.removeAll(skips);
			skips.clear();
			if (msgs.size() == 0) {
				return null;
			}
		}

		sb.append("<html content='text/html; charset=utf-8'>");
		sb.append("<style type='text/css'>.row  {clear:both;} .row>div{float:left;} .font12 {font-size:12px;} .font20 {font-size:20px;} .red {color:red;}</style>");
		sb.append("<body><H1>��������������ݿ������쳣,�뼰ʱ����!!!</H1>");
		for (Area area : areas) {
			StringBuffer asb = new StringBuffer();
			for (DBInfo dbInfo : dbinfos) {
				if (area.getId() != dbInfo.getAid()) {
					continue;
				}
				String html = msgs_html.get(dbInfo.getId());
				if (html != null) {
					asb.append(html);
				}
			}
			if (asb.length() > 0) {
				asb.insert(0, "<div class='row'><div><div class='font20'>[<b>"
						+ area.getName()
						+ "</b>]&nbsp;-&nbsp;</div></div><div>");
				asb.append("</div></div>");
				sb.append(asb);
			}

		}
		sb.append("</body></html>");
		return sb.toString();
	}
}
