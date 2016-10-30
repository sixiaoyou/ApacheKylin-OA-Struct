package com.njws.oalog;

import java.io.*;
import java.util.*;

/**
 * 类的说明 类名：FileOperation 作者：柏晨浩 日期：2016年9月18日 类的功能：完成对配置文件和kafka那边处理生成的文件的读取写入功能
 */
public class FileOperation {
	/**
	 * 存放主表时间列
	 */
	String timeName;
	/**
	 * 存放主表名
	 */
	String tableName;
	/**
	 * 存放配置文件中的cube名称
	 */
	String configCubeName;
	/**
	 * 存放最大时间配置文件中的cube名称
	 */
	String timeCubeName;
	/**
	 * 存放最近一批数据中的数据最大时间
	 */
	String lastMaxTime;
	/**
	 * 将配置文件中的cube存放至集合中
	 */
	Set<String> cubeSet = new HashSet<String>();
	/**
	 * key值为cube名，value值为主表名
	 */
	HashMap<String, String> cubeTable = new HashMap<String, String>();
	/**
	 * key值为cube名，value值为主表时间列
	 */
	HashMap<String, String> cubeTime = new HashMap<String, String>();
	/**
	 * key值为cube名，value值为最大时间
	 */
	HashMap<String, String> cubeMax = new HashMap<String, String>();
	/**
	 * 存放配置文件中项目名称
	 */
	String projectName;
	/**
	 * key值为cube名称，value值为cube对应的项目名称
	 */
	Map<String, String> cubeProject = new HashMap<String, String>();

	/**
	 * 存放主表信息
	 * 
	 * @author 柏晨浩
	 */
	class MainTableInfo {
		String table;

		public MainTableInfo(String table) {
			super();
			this.table = table;
		}
	}

	/**
	 * 追加写入：将所有job状态，错误job状态，最大时间配置文件中的内容覆盖写入至文件
	 * 
	 * @param jobLogFile
	 * @param jobStatus
	 */
	public void writeToFile(String file, String content) {
		BufferedWriter out = null;
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
			out.write(content);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 读取配置文件，配置文件中的第一列为cube名，第二列为主表名，第三列为主表对应的时间分区字段
	 * 
	 * @param readConfigFile
	 */
	public Set<String> readConfigFile(String configFile) {
		// 设置当前文件名字
		String filename = configFile;
		// 初始化读取文件操作
		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		String line = null;
		// 遍历读取文件
		try {
			fis = new FileInputStream(filename);
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			while ((line = br.readLine()) != null) {
				// 这里自己解析内容 line.spilt("\t"), 1是建模的表对应的cube名，2是建模的表关联的主表名
				String[] infos = line.split("\t");
				configCubeName = infos[0];
				tableName = infos[1];
				timeName = infos[2];
				cubeSet.add(configCubeName);
				cubeTime.put(configCubeName, timeName);
				cubeTable.put(configCubeName, tableName);

			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// 关闭文件操作
			try {
				br.close();
				isr.close();
				fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return cubeSet;
	}

	/**
	 * 最大时间配置文件，一共两列，第一列为cube名称，第二列为最近一批数据的各cube对应的最大时间
	 * 
	 * @param timeFile
	 * @return
	 */
	public String readTimeFile(String timeFile) {
		// 设置当前文件名字
		String filename = timeFile;
		// 初始化读取文件操作
		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		String line = null;
		// 遍历读取文件
		try {
			fis = new FileInputStream(filename);
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			while ((line = br.readLine()) != null) {
				String[] infos = line.split("\t");
				timeCubeName = infos[0];
				lastMaxTime = infos[1];
				cubeMax.put(timeCubeName, lastMaxTime);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// 关闭文件操作
			try {
				br.close();
				isr.close();
				fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return lastMaxTime;
	}
}
