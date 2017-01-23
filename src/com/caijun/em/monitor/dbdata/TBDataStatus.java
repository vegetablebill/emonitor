package com.caijun.em.monitor.dbdata;

import java.util.Date;

public class TBDataStatus {
	private long id;
	private long tbid;
	private long srcSum;
	private long srcNeedChangeSum;
	private Date srcNeedChangeMinT;
	private long srcNeedDeleteSum;
	private Date srcNeedDeleteMinT;
	private long destSum;
	private Date ct;

	public TBDataStatus(long id, long tbid, long srcSum, long srcNeedChangeSum, Date srcNeedChangeMinT,
			long srcNeedDeleteSum, Date srcNeedDeleteMinT, long destSum, Date ct) {
		super();
		this.id = id;
		this.tbid = tbid;
		this.srcSum = srcSum;
		this.srcNeedChangeSum = srcNeedChangeSum;
		this.srcNeedChangeMinT = srcNeedChangeMinT;
		this.srcNeedDeleteSum = srcNeedDeleteSum;
		this.srcNeedDeleteMinT = srcNeedDeleteMinT;
		this.destSum = destSum;
		this.ct = ct;
	}

	public long getId() {
		return id;
	}

	public long getTbid() {
		return tbid;
	}

	public long getSrcSum() {
		return srcSum;
	}

	public long getSrcNeedChangeSum() {
		return srcNeedChangeSum;
	}

	public Date getSrcNeedChangeMinT() {
		return srcNeedChangeMinT;
	}

	public long getSrcNeedDeleteSum() {
		return srcNeedDeleteSum;
	}

	public Date getSrcNeedDeleteMinT() {
		return srcNeedDeleteMinT;
	}

	public long getDestSum() {
		return destSum;
	}

	public Date getCt() {
		return ct;
	}

}
