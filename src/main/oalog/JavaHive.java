package com.njws.oalog;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * 通过Java与Hive交互，主要执行两条SQL语句
 * 
 * @author 柏晨浩
 */
public class JavaHive {
	/**
	 * 存放本次查询的最大时间
	 */
	StringBuffer maxTimeBuf = new StringBuffer();
	/**
	 * 记录日志操作
	 */
	private static Logger logger = Logger.getLogger(JavaHive.class);
	/**
	 * Map嵌套，以存放不同的cube对应的多个时间
	 */
	Map<String, Map<String, LogTime>> initMap = new HashMap<String, Map<String, LogTime>>();

	/**
	 * @author 柏晨浩 待操作数据的时间列
	 */
	class LogTime {
		String dataTime;

		public LogTime(String dataTime) {
			super();
			this.dataTime = dataTime;
		}
	}

	/**
	 * 查询不同cube对应的需更新的数据
	 * 
	 * @param args
	 * @throws SQLException
	 */
	public void queryDataTime(String cubeName, String lastMaxTime, String timeColumn, String tableName,
			String kylinIp) {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String queryPeriodTime = "select distinct(substr(" + timeColumn + ",0,10)) from " + tableName + " where substr("
				+ timeColumn + ",0,19) > " + '\"' + lastMaxTime + '\"';
		logger.info("本次查询带刷新SQL数据语句为: " + queryPeriodTime);
		Connection con;
		try {
			con = DriverManager.getConnection("jdbc:hive2://" + kylinIp + "/logcenter", "", "");
			Statement stmt = con.createStatement();
			ResultSet resPeriodTime = stmt.executeQuery(queryPeriodTime);
			while (resPeriodTime.next()) {
				LogTime logtime = new LogTime(resPeriodTime.getString(1));
				if (initMap.get(cubeName) == null) {
					initMap.put(cubeName, new HashMap<String, LogTime>());
				}
				initMap.get(cubeName).put(resPeriodTime.getString(1), logtime);
				logger.info("查询结果为: " + cubeName + "-------" + resPeriodTime.getString(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 查询每个cube对应的当前的最大时间
	 * 
	 * @param cubeName
	 * @param timeColumn
	 * @param tableName
	 * @param kylinIp
	 * @return
	 */
	public StringBuffer queryMaxTime(String cubeName, String timeColumn, String tableName, String kylinIp) {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String queryMaxTime = "select MAX(substr(" + timeColumn + ",0,19)) from " + tableName;
		logger.info("本次查询最大时间SQL语句为: " + queryMaxTime);
		Connection con;
		try {
			con = DriverManager.getConnection("jdbc:hive2://" + kylinIp + "/logcenter", "", "");
			Statement stmt = con.createStatement();
			ResultSet resMaxTime = stmt.executeQuery(queryMaxTime);
			while (resMaxTime.next()) {
				maxTimeBuf.append(cubeName + "\t" + resMaxTime.getString(1) + "\r\n");
				logger.info("新的最大时间为: " + cubeName + ":" + resMaxTime.getString(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return maxTimeBuf;
	}
}
