package com.njws.oalog;

import com.njws.oalog.JsonCompilation;
import com.njws.oalog.FileOperation;
import com.njws.oalog.JsonCompilation.JobId;
import com.njws.oalog.TimeOperation;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import org.apache.log4j.Logger;
import util.PropertyUtil;

public class JobError extends TimerTask {
	/**
	 * Refresh操作调用Kylin的返回buf值
	 */
	StringBuffer refreshBuffer = new StringBuffer();
	/**
	 * 新增操作调用Kylin的返回buf值
	 */
	StringBuffer addBuffer = new StringBuffer();
	/**
	 * 需要Refresh的数据
	 */
	Set<String> jobRefreshSet = new HashSet<String>();
	/**
	 * 需要新增数据的集合
	 */
	Set<String> jobAddSet = new HashSet<String>();

	/**
	 * key值segment开始时间，value值segment结束时间
	 */
	Map<String, String> jobRefreshMap = new HashMap<String, String>();
	/**
	 * Key值请求新增操作的时间，value值该时间的下一天
	 */
	Map<String, String> jobAddMap = new HashMap<String, String>();
	/**
	 * key值jobStatus，value值为以jobUuid为key，jobId为value的map
	 */
	Map<String, Map<String, JobId>> jobIdMap = new HashMap<String, Map<String, JobId>>();

	/**
	 * 实例化Oalog外部类
	 */
	static OaLog ol = new OaLog();
	/**
	 * 实例化文件操作
	 */
	static FileOperation rw = new FileOperation();
	/**
	 * 实例化守护进程---侦听job状态的进程，一但job出现error状态，则discard掉该job，在执行该job先前执行的相应操作。
	 */
	static JobError je = new JobError();
	/**
	 * 实例化时间操作的外部类
	 */
	static TimeOperation time = new TimeOperation();
	/**
	 * 实例化KylinApi类
	 */
	static KylinApi api = new KylinApi();
	/**
	 * 记录日志操作
	 */
	private static Logger logger = Logger.getLogger(JobError.class);
	/**
	 * 实例化解析Json字符串的外部类
	 */
	static JsonCompilation json = new JsonCompilation();

	/**
	 * 加载配置文件
	 */
	public JobError() {
		super();
		PropertyUtil.loadConfig();
	}

	/**
	 * 判断segment是否存在，进而判断该job需要进行的操作
	 * 
	 * @param refreshBuf
	 * @return
	 */
	public boolean isSegmentExist(String refreshBuf) {
		String searchChars = "exception";
		return refreshBuf.contains(searchChars);
	}

	/**
	 * 发现处于“ERROR状态的job。将其drop掉，并进行原先的操作
	 * 
	 * @param jobId
	 * @param kylinIp
	 */
	public void discardJob(String jobId, String params) {
		api.discard(jobId, params);
	}

	/**
	 * 拟定每30分钟侦听一次
	 */
	public void run() {
		rw.readConfigFile(PropertyUtil.prop.getProperty("configFile"));
		for (String cubeName : rw.cubeSet) {
			logger.info(cubeName);
			json.compileJobJson(cubeName);// 获取一个cube下的job状态
			Map<String, JsonCompilation.JobId> UuidMap = json.jobIdMap.get("ERROR");
			logger.info("当前检测的cube名称:" + cubeName);
			if (UuidMap != null) {
				Set<String> jobIdSet = UuidMap.keySet();
				for (String jobIdSet1 : jobIdSet) {
					je.discardJob(jobIdSet1, null);
					String jobName = json.jobSegmentMap.get(jobIdSet1);
					logger.info("处于ERROR状态的job: " + jobIdSet1 + "  " + jobName);
					time.getSegmentTime(jobName);
					if (je.isSegmentExist(ol.refreshCube(time.startTime, time.endTime, cubeName).toString())) {
						ol.addSegment(time.startTime, time.endTime, cubeName);
					}
				}
			}
		}
		rw.writeToFile(PropertyUtil.prop.getProperty("joboaerrorlog"), json.jobErrorBuf.toString());// Error状态job持久化到日志
		rw.writeToFile(PropertyUtil.prop.getProperty("joboalog"), json.jobLogBuf.toString());// job状态持久化到日志
		json.jobErrorBuf.setLength(0);
		json.jobLogBuf.setLength(0);
	}
}
