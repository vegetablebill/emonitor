package com.caijun.em.monitor.dbdata;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.caijun.em.Systore;
import com.caijun.em.monitor.dbschema.TBInfo;

public class TBDataAnalyzer {
	private final static String CHANGEOUTTIME = "TBData.changeOutTime";
	private Systore systore;
	private Map<Long, TBDataStatus> curTBDataStatus;

	public TBDataAnalyzer(Systore systore, Map<Long, TBDataStatus> curTBDataStatus) {
		super();
		this.systore = systore;
		this.curTBDataStatus = curTBDataStatus;
	}

	public List<TBDataStatus> doing() {
		List<TBDataStatus> result = new ArrayList<TBDataStatus>();
		long compareTime = new Date().getTime() - systore.props.getInMinLimit(CHANGEOUTTIME, 30, 0) * 6000;
		for (TBInfo tbInfo : systore.dbSchema.getTBInfos()) {
			TBDataStatus status = curTBDataStatus.get(tbInfo.getId());
			if (status != null && compareTime >= status.getCt().getTime()
					&& (status.getSrcNeedChangeSum() > 0 || status.getSrcNeedDeleteSum() > 0)) {
				result.add(status);
			}
		}
		return result;
	}
}
