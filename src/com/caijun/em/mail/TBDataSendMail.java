package com.caijun.em.mail;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.w3c.dom.Node;

import com.caijun.em.Area;
import com.caijun.em.Systore;
import com.caijun.mail.Email;
import com.caijun.utils.str.StringUtil;
import com.caijun.utils.xml.XMLHandler;

public class TBDataSendMail extends SendMail {
	private static String MAIL_TYPE_TBDATA = "tbdata";
	private static String MAIL_TO = "Mail.TBData.ToUser";
	private static String MAIL_REPEAT_INTERVAL = "Mail.TBData.Repeat_Interval";
	private Map<String, Msg> tbdata_LSMS;
	private String mailContent;
	private List<Msg> msgs;

	public TBDataSendMail(Systore systore, Logger logger) {
		super(systore, logger);
		tbdata_LSMS = new HashMap<String, Msg>();
		Date begin = new Date(
				new Date().getTime() - systore.props.getInMinLimit(MAIL_REPEAT_INTERVAL, 180L, 1L) * 60000);
		List<Msg> msgs = this.getLatestSendMS(MAIL_TYPE_TBDATA, begin);
		for (Msg msg : msgs) {
			tbdata_LSMS.put(msg.digest, msg);
		}
	}

	@Override
	public void beforeSend() {
		if (logger != null) {
			logger.info("上次处理过数据库表数据交换异常消息" + tbdata_LSMS.values().size() + "条"
					+ (!logger.isDebugEnabled() ? "." : ",消息ID为:" + serializeMsgByID(tbdata_LSMS.values())));
		}
		msgs = getNeedSendMSG();
		if (logger != null) {
			logger.info("需要发送邮件的数据库表数据交换异常消息" + msgs.size() + "条"
					+ (!logger.isDebugEnabled() ? "." : ",消息ID为:" + serializeMsgByID(msgs)));
		}

		if (msgs == null || msgs.size() == 0) {
			return;
		}
		mailContent = createMailContent(msgs);
	}

	@Override
	public Email send() {
		if (msgs == null || msgs.size() == 0) {
			return null;
		}
		String user = systore.props.get(MAIL_USER);
		String to = systore.props.get(MAIL_TO);
		Email email = Email.create().from(user).to(to);
		email.subject("数据库表数据交换异常提醒");
		email.addHtml(mailContent);
		return email;
	}

	@Override
	public void afterSend(String emailID) {
		if (msgs.size() == 0 && logger != null) {
			logger.info("没有发送数据库表数据交换异常邮件.");
		} else if (logger != null) {
			logger.info("发送数据库表数据交换异常邮件成功,邮件编号[" + emailID + "]");
		}
		setSended(msgs);
		long timeFlag = new Date().getTime() - systore.props.getInMinLimit(MAIL_REPEAT_INTERVAL, 180L, 1L) * 60000;
		for (Msg msg : tbdata_LSMS.values()) {
			if (timeFlag > msg.ct.getTime()) {
				tbdata_LSMS.remove(msg.digest);
			}
		}
		for (Msg msg : msgs) {
			tbdata_LSMS.put(msg.digest, msg);
		}

	}

	private List<Msg> getNeedSendMSG() {
		boolean sendFlag = false;
		List<Msg> rs = new ArrayList<Msg>();
		List<Msg> skipMsgs = this.getUndealMS(MAIL_TYPE_TBDATA);
		logger.info("本次需要处理的数据库表数据交换异常消息" + skipMsgs.size() + "条"
				+ (!logger.isDebugEnabled() ? "." : ",消息ID为:" + serializeMsgByID(skipMsgs)));
		long time = systore.props.getInMinLimit(MAIL_REPEAT_INTERVAL, 180L, 1L) * 60000;
		Map<String, List<Msg>> msgs_map = this.groupByDigest(skipMsgs);
		for (String key : msgs_map.keySet()) {
			Msg msg = this.getNewestOne(msgs_map.get(key));
			rs.add(msg);
			if (!tbdata_LSMS.containsKey(key) || msg.ct.getTime() - tbdata_LSMS.get(key).ct.getTime() >= time) {
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

	private String createMailContent(List<Msg> msgs) {
		StringBuffer sb = new StringBuffer();
		List<Area> areas = systore.getAreas();
		Map<String, Map<String, StringBuffer>> tbdata_htmls = new HashMap<String, Map<String, StringBuffer>>();
		List<Msg> skips = new ArrayList<Msg>();
		for (Msg msg : msgs) {
			try {
				Node m = XMLHandler.getSubNode(com.caijun.utils.xml.XMLHandler.loadXMLString(msg.content), "m");
				String area_name = XMLHandler.getTagValue(m, "an");
				String db_name = XMLHandler.getTagValue(m, "dn");
				String tb_name = XMLHandler.getTagValue(m, "tn");
				String src_num = XMLHandler.getTagValue(m, "srcn");
				String dest_num = XMLHandler.getTagValue(m, "destn");
				String src_nc_num = XMLHandler.getTagValue(m, "srcncn");
				String src_nc_t = XMLHandler.getTagValue(m, "srcnct");
				String src_nd_num = XMLHandler.getTagValue(m, "srcndn");
				String src_nd_t = XMLHandler.getTagValue(m, "srcndt");
				String ct = XMLHandler.getTagValue(m, "ct");

				if (!tbdata_htmls.containsKey(area_name)) {
					tbdata_htmls.put(area_name, new HashMap<String, StringBuffer>());
				}
				Map<String, StringBuffer> map_tmp = tbdata_htmls.get(area_name);
				if (!map_tmp.containsKey(db_name)) {
					map_tmp.put(db_name, new StringBuffer("<div><div><div><span class='font20'>[" + db_name
							+ "]</span><span class='font12'>数据库:</span></div><div class='tbs'><div><table><tr><td>表名称</td><td>源数据量</td><td>需要交换量</td><td>中心数据量</td><td>采样时间</td></tr>"));
				}
				StringBuffer sb_tmp = map_tmp.get(db_name);
				sb_tmp.append("<tr><td>").append(tb_name).append("</td><td>").append(src_num).append("</td><td>")
						.append("<div><div class='div1'>(增/改)</div><div class='div2'>").append(src_nc_num).append("&nbsp;&nbsp;</div><div>")
						.append(src_nc_t).append("</div></div><div><div class='div1'>(减)</div><div class='div2'>").append(src_nd_num)
						.append("&nbsp;&nbsp;</div><div>").append(src_nd_t).append("</div></div></td><td>").append(dest_num)
						.append("</td><td>").append(ct).append("</td></tr>");
			} catch (Exception e) {
				logger.error("消息[" + msg.id + "]解析异常,丢弃", e);
				skips.add(msg);
			}
		}
		for (Map<String, StringBuffer> map_tmp : tbdata_htmls.values()) {
			for (StringBuffer sb_tmp : map_tmp.values()) {
				sb_tmp.append("</table></div></div></div></div>");
			}
		}
		this.skipMsgs(skips);
		msgs.removeAll(skips);
		if (msgs.size() == 0) {
			return null;
		}

		sb.append("<html content='text/html; charset=gb2312'>");
		sb.append(
				"<style type='text/css'>body {word-wrap:break-word;word-break:normal;} .area {clear:both;} .area>div {float:left;} .font12 ,table{font-size:12px;} .font20 {font-size:20px;font-weight:bold} .tbs {margin-left:25px;width:850px;} .tbs>div {margin-top:5px;margin-bottom:10px;} table {width:100%;border-collapse:collapse;border: 1px solid #ff00ff;} td {padding:2px;border: 1px solid #ff00ff;text-align:left;vertical-align:top;} td>div{clear:both;} td div>div{float:left;} .div1 {width:50px;} .div2 {width:100px;} td:nth-child(1),td:nth-child(2),td:nth-child(4),td:nth-child(5) {width:15%;} .gred {background-color:yellow;font-weight:bold;} .bold {font-weight:bold;}</style>");
		sb.append("<body><H1>以下区域存在数据库表数据交换异常,请及时处理!!!</H1>");

		for (Area area : areas) {
			Map<String, StringBuffer> htmls = tbdata_htmls.get(area.getName());
			if (htmls == null) {
				continue;
			}
			sb.append("<div class='area'><div><div class='font20'>[" + area.getName() + "]&nbsp;-&nbsp;</div></div><div>");
			for (StringBuffer sb1 : htmls.values()) {
				sb.append(sb1.toString());
			}
			sb.append("</div></div>");
		}
		sb.append("</body></html>");
		return sb.toString();
	}
}
