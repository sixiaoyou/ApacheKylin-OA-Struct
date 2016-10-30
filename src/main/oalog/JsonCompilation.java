package com.njws.oalog;

import com.jcraft.jsch.JSchException;
import util.PropertyUtil;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.json.*;

/**
 * 类的说明 类名：JsonCompilation 作者：柏晨浩 时间：2016年9月18日 类的功能：调用kylin接口，对buf中的返回json串进行解析
 */
public class JsonCompilation {

	/**
	 * 存放getSegment 返回buf的json数组大小
	 */
	int sSize;
	/**
	 * 单个segment的开始时间
	 */
	String start;
	/**
	 * 单个segment的结束时间
	 */
	String end;
	/**
	 * 一个cube中的所有segment的开始时间
	 */
	String[] segStartTime;
	/**
	 * 一个cube中的所有segment的结束时间
	 */
	String[] segEndTime;
	/**
	 * 存放getSegment的返回buf
	 */
	StringBuffer segBuf = new StringBuffer();
	/**
	 * 获取查询job返回buf的json数返回组长度
	 */
	int iSize;
	/**
	 * 系统时间
	 */
	String systemTime;
	/**
	 * jobId
	 */
	String[] jobUuid;
	/**
	 * 返回jobBuf
	 */
	StringBuffer jobBuf = new StringBuffer();
	/**
	 * 存放job状态信息
	 */
	StringBuffer jobLogBuf = new StringBuffer();
	/**
	 * 存放joberror状态信息
	 */
	StringBuffer jobErrorBuf = new StringBuffer();
	/**
	 * 实例化文件读写操作
	 */
	FileOperation rw = new FileOperation();
	/**
	 * 实例化KylinApi类
	 */
	KylinApi api = new KylinApi();
	/**
	 * 记录日志操作
	 */
	private static Logger logger = Logger.getLogger(JsonCompilation.class);
	/**
	 * key值jobStatus，value值为以jobUuid为key，jobId为value的map
	 */
	Map<String, Map<String, JobId>> jobIdMap = new HashMap<String, Map<String, JobId>>();
	/**
	 * key值为jobUuid，value值为jobName
	 */
	Map<String, String> jobSegmentMap = new HashMap<String, String>();

	/**
	 * 存放jobId
	 */
	class JobId {
		String JobId;

		public JobId(String JobId) {
			super();
			this.JobId = JobId;
		}
	}

	public JsonCompilation() {
		super();
		PropertyUtil.loadConfig();
	}
	
	/**
	 * 	获取某个cube的信息，目的在于获取该cube已有的segment。
	 * @param cubeName
	 * @return
	 */
	public StringBuffer getSegment(String cubeName,String params) {
			logger.info("获取某个cube的segment:   "+"cubename:  "+cubeName);
			StringBuffer segment = new StringBuffer();
			segment = api.getSeg(cubeName, params);
		    return segment;
	}

	/**
	 * 获取某个cube中的所有job的id
	 * 
	 * @return
	 * @throws JSchException
	 * @throws IOException
	 */
	public StringBuffer getJobId(String cubeName, String params) {
		StringBuffer job = new StringBuffer();
		job=api.getJob(cubeName, params);
		return job;
	}

	/**
	 * 获取系统时间并将其格式化
	 * 
	 * @return
	 */
	public String time() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式
		systemTime = df.format(new Date());
		logger.info("json-Time  " + "systemTime:  " + systemTime);
		return systemTime;
	}

	/**
	 * 获取有关Kylin job的Json数组
	 * 
	 * @param projectName
	 * @param cubeName
	 */
	public void compileJobJson(String cubeName) {
		String systemTime = time();
		jobBuf = getJobId(cubeName,null);
		String jobString = jobBuf.toString();
		JSONArray jsonArray = null;
		try {
			jsonArray = new JSONArray(jobString);
		} catch (JSONException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		iSize = jsonArray.length();
		// 获得json数组中，uuid、job_status、name后的值
		for (int i = 0; i < iSize; i++) {
			JSONObject jsonObj = null;
			try {
				jsonObj = jsonArray.getJSONObject(i);
			} catch (JSONException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			String jobUuid = null;
			try {
				jobUuid = jsonObj.get("uuid") + "";
			} catch (JSONException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			String jobStatus = null;
			try {
				jobStatus = jsonObj.get("job_status") + "";
			} catch (JSONException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			String jobName = null;
			try {
				jobName = jsonObj.get("name") + "";
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String jobLog = "[" + i + "]" + " " + systemTime + " " + "jobName: " + jobName + " " + "jobUuid= " + jobUuid
					+ "	 " + "jobStatus:" + " " + jobStatus + "\r\n";
			if (jobStatus.equals("ERROR")) {
				JobId jobId = new JobId(jobStatus);
				if (jobIdMap.get(jobStatus) == null) {
					jobIdMap.put(jobStatus, new HashMap<String, JobId>());
				}
				jobIdMap.get(jobStatus).put(jobUuid, jobId);
				jobSegmentMap.put(jobUuid, jobName);
				jobErrorBuf.append(jobLog + "\r\n");
			}
			jobLogBuf.append(jobLog + "\r\n");
		}
	}

	/**
	 * 解析cube中Segment的json数组
	 * 
	 * @param cubeName
	 */
	public void compileSegmentJson(String cubeName) {
		segBuf = getSegment(cubeName, null);
		String segString = segBuf.toString();
		// 返回的buf内容并不是一个规范的json数组，这里需要截取一下
		String segSubString = segString.substring(segString.indexOf("[{\"uuid"),
				segString.indexOf(",\"last_modified\""));
		JSONArray segJsonArray = null;
		try {
			segJsonArray = new JSONArray(segSubString);
		} catch (JSONException e4) {
			// TODO Auto-generated catch block
			logger.info(e4.getMessage());
		}
		sSize = segJsonArray.length();// json数组的长度
		segStartTime = new String[sSize];// segment的开始时间
		segEndTime = new String[sSize];// segment的结束时间

		for (int i = 0; i < sSize; i++) {
			JSONObject segJsonObj = null;
			try {
				segJsonObj = segJsonArray.getJSONObject(i);
			} catch (JSONException e3) {
				// TODO Auto-generated catch block
				logger.info(e3.getMessage());
			}
			Calendar c = Calendar.getInstance();
			Calendar a = Calendar.getInstance();
			String segStartString = "";
			try {
				segStartString = segJsonObj.get("date_range_start") + "";
			} catch (JSONException e2) {
				// TODO Auto-generated catch block
				logger.info(e2.getMessage());
			} // segment中的开始时间对json数组中的date_range_start后的内容
			String segEndString = null;
			try {
				segEndString = segJsonObj.get("date_range_end") + "";
			} catch (JSONException e1) {
				// TODO Auto-generated catch block
				logger.info(e1.getMessage());
			} // segment中的结束时间对json数组中的date_range_end后的内容
			Long segStartLong = Long.valueOf(segStartString);
			Long segEndLong = Long.valueOf(segEndString);
			c.setTimeInMillis(segStartLong);
			a.setTimeInMillis(segEndLong);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date startDate = c.getTime();
			Date endDate = a.getTime();
			start = sdf.format(startDate);
			end = sdf.format(endDate);
			segStartTime[i] = start;
			segEndTime[i] = end;
		}
	}
}
