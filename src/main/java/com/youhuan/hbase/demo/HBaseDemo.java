/**
 *  软件版权：UMPAY
 *  系统名称：HBaseTest
 *  文件名称：HBaseDemo.java
 *  版本变更记录（可选）：修改日期2018年8月24日  下午4:37:11，修改人Administrator，工单号（手填），修改描述（手填）
 */
package com.youhuan.hbase.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
/** 
 * @Description：
 * <p>创建日期：2018年8月24日 </p>
 * @version V1.0  
 * @author Administrator
 * @see
 */
public class HBaseDemo {
	
	private static Configuration conf = null;
	
	static {
		conf = HBaseConfiguration.create();
		
	}
	
	public static boolean isExistTables(String tableName) throws Exception {
		//操作hbase必须创建HBaseAdmin对象
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
		return hBaseAdmin.tableExists(tableName);
		
	}
	public static void main(String[] args) {
		
		try {
//			boolean existTables = isExistTables("student");
			boolean existTables = isExistTables("student2");
			System.out.println(existTables);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
}
