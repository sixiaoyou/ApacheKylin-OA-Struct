package com.njws.oalog;

import com.njws.oalog.JsonCompilation;
import com.njws.oalog.JavaHive.LogTime;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import util.PropertyUtil;
import org.apache.log4j.Logger;


/**
 * OAlog每日更新
 * @author 柏晨浩
 *
 */
public class OaLog extends TimerTask{

		/**
		 * 新增操作调用Kylin的返回buf值
		 */
		StringBuffer addBuf = new StringBuffer();
		/**
		 * Refresh操作调用Kylin的返回buf值
		 */
		StringBuffer refreshBuf = new StringBuffer();
		/**
		 * 存放cube名称
		 */
		Set<String>cubeSet = new HashSet<String>();
		/**
		 * 实例化文件操作类
		 */
		static FileOperation rw = new FileOperation();
		/**
		 * 实例化Json解析外部类
		 */
		static JsonCompilation json = new JsonCompilation();
		/**
		 * 实例化检查job ERROR状态的外部类
		 */
		static JobError je = new JobError();
		/**
		 * 实例化时间操作外部类
		 */
		static TimeOperation time = new TimeOperation();
		/**
		 * 实例化KylinApi类
		 */
		static KylinApi api = new KylinApi();
		/**
		 * key为数据时间，value为该数据时间对应seg的开始时间
		 */
		Map<String,String> refreshStartMap=new HashMap<String,String>();
		/**
		 * key为segment开始时间，value为segment结束时间；
		 */
		Map<String,String> refreshMap=new HashMap<String,String>(); 

		/**
		 * 记录日志操作
		 */
		private static Logger logger = Logger.getLogger(OaLog.class); 
		/**
		 * 导入配置文件路径
		 */
		public OaLog() {
			super();
			PropertyUtil.loadConfig();
		}

/**
 * Refresh操作
 * @param refreshStarttime 
 * @param refreshEndtime
 * @param refreshCube
 * @return refreshBuffer
 */
public StringBuffer refreshCube(String refreshStartTime, String refreshEndTime,String cubeName) {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			logger.info("Refresh操作: cube名称:  "+cubeName+"  "+"开始时间: " + refreshStartTime + "   " + "结束时间: " + refreshEndTime);
			try {
				Date startTime = format.parse(refreshStartTime + " " + "08:00:00");
				Date endTime = format.parse(refreshEndTime + " " + "08:00:00");
				refreshBuf = api.refresh(startTime,endTime,cubeName); 
			} catch (ParseException e) {
				logger.info(e.getMessage());
			}
			return refreshBuf;
}
/**
 * Build操作，时间格式为"yyyy-MM-dd",开始时间为今天,结束时间为明天
 * @param addStartTime
 * @param addEndTime
 * @return
 */
public StringBuffer addSegment(String addStartTime, String addEndTime,String cubeName){
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			logger.info("新增操作: cube名称:  "+cubeName+"   "+"开始时间: "+addStartTime+"   "+"结束时间: "+addEndTime);
			try {
				Date addStart = format.parse(addStartTime + " " + "08:00:00");
				Date addEnd = format.parse(addEndTime + " " + "08:00:00");
				addBuf = api.add(addStart,addEnd,cubeName);
			} catch (ParseException e) {
				logger.info(e.getMessage());
			} 
			return  addBuf;
		}
/**
 * 将经Kafka到HBase处理过后的数据分为需Refresh操作和需新增操作这两批
 * @param cubeName
 * @param dataTime
 */
public boolean hasSegment(String cubeName, String dataTime) {
			json.compileSegmentJson(cubeName);
			boolean ok=false;
			for (int j = 0; j < json.sSize; j++) {
				if (dataTime.compareTo(json.segStartTime[j]) >= 0 && dataTime.compareTo(json.segEndTime[j]) < 0) {
					refreshStartMap.put(dataTime, json.segStartTime[j]);
					refreshMap.put(json.segStartTime[j],json.segEndTime[j]);
					ok=true;
				}
			}
			return ok;
}			

/**
 * 拟定每一小时执行一次
 */
public void run(){
			JavaHive jh = new JavaHive();
			cubeSet=rw.readConfigFile(PropertyUtil.prop.getProperty("configFile"));
			rw.readTimeFile(PropertyUtil.prop.getProperty("timeFile"));
			StringBuffer  newMaxTimeBuf = new StringBuffer();
				for(String cubeName:cubeSet){
					System.out.println(cubeName);
					jh.queryDataTime(cubeName,rw.cubeMax.get(cubeName), rw.cubeTime.get(cubeName),rw.cubeTable.get(cubeName),PropertyUtil.prop.getProperty("hiveIp"));
					newMaxTimeBuf=jh.queryMaxTime(cubeName,rw.cubeTime.get(cubeName),rw.cubeTable.get(cubeName),PropertyUtil.prop.getProperty("hiveIp"));
					Map<String,LogTime> logTimeMap=jh.initMap.get(cubeName);
					  if (logTimeMap!=null) {
					      Set<String> logTimeSet=logTimeMap.keySet();
					      for (String logTime1:logTimeSet) {
					    		if (hasSegment(cubeName, logTime1)) {
									refreshCube(refreshStartMap.get(logTime1), refreshMap.get(refreshStartMap.get(logTime1)),
											cubeName);
								} else {
									addSegment(time.getCurrentYearFirst(),time.getNextYearFirst(),cubeName);
								}
					      }
					  }
					}	
				if(newMaxTimeBuf.toString()!="\t\r\n"){
				  rw.writeToFile(PropertyUtil.prop.getProperty("timeFile"),newMaxTimeBuf.toString());
				}
				newMaxTimeBuf.setLength(0);
		}

public static void main(String[] args) { 
			 Timer timer = new Timer(); 
			 long delay1 =Long.parseLong(PropertyUtil.prop.getProperty("delay1")); 
			 long period1 =Long.parseLong(PropertyUtil.prop.getProperty("period1"));
			 // 从现在开始 1 秒钟之后，每隔60分钟执行一次 job1 
			 timer.schedule(new OaLog(), delay1, period1); 
			 
			 long delay2 = Long.parseLong(PropertyUtil.prop.getProperty("delay2")); 
			 long period2 = Long.parseLong(PropertyUtil.prop.getProperty("period2"));
			 // 从现在开始 1 秒钟之后，每隔30分钟执行一次 job1 
			 timer.schedule(new JobError(), delay2, period2); 
			 } 
	}
