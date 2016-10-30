package com.njws.oalog;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.log4j.Logger;


/**类的说明
 * 类名：TimeOperation
 * 作者：柏晨浩
 * 时间：2016年9月18日
 * 类的功能：包含一些关于时间的方法
 */
public class TimeOperation{
		/**
		 * ERROR job对应segment的开始时间
		 */
		String startTime;
		/**
		 * ERROR job对应segment的结束时间
		 */
		String endTime;
		/**
		 * 记录日志操作
		 */
		private static Logger logger = Logger.getLogger(TimeOperation.class);  
		
		private int getYearPlus() {
			Calendar cd = Calendar.getInstance();
			int yearOfNumber = cd.get(Calendar.DAY_OF_YEAR);// 获得当天是一年中的第几天
			cd.set(Calendar.DAY_OF_YEAR, 1);// 把日期设为当年第一天
			cd.roll(Calendar.DAY_OF_YEAR, -1);// 把日期回滚一天。
			int MaxYear = cd.get(Calendar.DAY_OF_YEAR);
			if (yearOfNumber == 1) {
				return -MaxYear;
			} else {
				return 1 - yearOfNumber;
			}
		}

		// 获得本年第一天的日期
		public String getCurrentYearFirst() {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			int yearPlus = this.getYearPlus();
			
			GregorianCalendar currentDate = new GregorianCalendar();
			currentDate.add(GregorianCalendar.DATE, yearPlus);
			Date yearDay = currentDate.getTime();
			String preYearDay = sdf.format(yearDay);
			return preYearDay;
		}

		// 获得明年第一天的日期
		public String getNextYearFirst() {
			String nextYearDay = "";
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

			Calendar lastDate = Calendar.getInstance();
			lastDate.add(Calendar.YEAR, 1);// 加一个年
			lastDate.set(Calendar.DAY_OF_YEAR, 1);
			nextYearDay = sdf.format(lastDate.getTime());
			return nextYearDay;
		}
		
/**
 * 获取处于error状态的job对应的segment
 * @param jobName
 */
public void getSegmentTime(String jobName){
	String jobString = jobName.substring(jobName.indexOf("-"),jobName.indexOf("T"));
	String jobStartTime = jobString.substring(2,10);//取得segment开始时间
	String jobEndTime = jobString.substring(17,25);//取得segment结束时间
	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
	SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
	Date date1 = null;
	Date date2 = null;
	try {
		date1 = sdf.parse(jobStartTime);
		date2 = sdf.parse(jobEndTime);
	} catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}			
	startTime = sdf1.format(date1);
	endTime = sdf1.format(date2);
	}
}

