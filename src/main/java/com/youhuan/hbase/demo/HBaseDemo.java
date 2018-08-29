/**
 *  软件版权：UMPAY
 *  系统名称：HBaseTest
 *  文件名称：HBaseDemo.java
 *  版本变更记录（可选）：修改日期2018年8月24日  下午4:37:11，修改人Administrator，工单号（手填），修改描述（手填）
 */
package com.youhuan.hbase.demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
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
	
	/**
	 * @Description：表是否存在
	 * <p>创建人：Administrator ,  2018年8月28日  上午10:07:24</p>
	 * <p>修改人：Administrator ,  2018年8月28日  上午10:07:24</p>
	 *
	 * @param tableName
	 * @return
	 * @throws Exception
	 * boolean 
	 */
	public static boolean isExistTables(String tableName) throws Exception {
		//操作hbase必须创建HBaseAdmin对象
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
		return hBaseAdmin.tableExists(tableName);
		
	}
	public static void main(String[] args) {
		
		try {
			
//			List<String> paramsList=new ArrayList<String>();
//			paramsList.add("info");
//			paramsList.add("score");
//			String[] paramArr=new String[paramsList.size()]  ;
//			paramsList.toArray(paramArr);
//			createTable("stuff",paramArr);
//			addRow("stuff", "1001", "info", "name", "youhuan");
//			addRow("stuff", "1001", "info", "sex", "male");
			
//			getRow("stuff", "1001");
//			getRowQuality("stuff", "1001","info22", "sex");
			getAllRow("stuff");
//			dropTable("stuff");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
	
	/**
	 * @Description：创建表
	 * <p>创建人：Administrator ,  2018年8月28日  上午10:07:08</p>
	 * <p>修改人：Administrator ,  2018年8月28日  上午10:07:08</p>
	 *
	 * @param tableName
	 * @param columnFamily
	 * @throws Exception
	 * void 
	 */
	public static void createTable(String tableName,String... columnFamily) throws Exception {
		
		if(isExistTables(tableName)) {
			System.out.println("表已经存在！！！");
		}else {
			HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));		
			System.out.println("开始创建表！！！"+tableName);
			for (String cf : columnFamily) {
				hTableDescriptor.addFamily(new HColumnDescriptor(cf));
			}
			HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
			hBaseAdmin.createTable(hTableDescriptor);
			hBaseAdmin.close();
		}
		
	} 
	
	/**
	 * @Description：删除表
	 * <p>创建人：Administrator ,  2018年8月28日  上午10:06:52</p>
	 * <p>修改人：Administrator ,  2018年8月28日  上午10:06:52</p>
	 *
	 * @param tableName
	 * @throws Exception
	 * void 
	 */
	public static void dropTable(String tableName) throws Exception {
		if(!isExistTables(tableName)) {
			System.out.println("表不存在！！！");
		}else {
			HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));		
			System.out.println("开始删除表！！！"+tableName);
			HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
			//1.先关闭表
			hBaseAdmin.disableTable(tableName);
			//2.再删除表
			hBaseAdmin.deleteTable(tableName);
			hBaseAdmin.close();
		}
		
	} 
	
	
	/**
	 * @Description：添加一行
	 * <p>创建人：Administrator ,  2018年8月28日  下午5:16:30</p>
	 * <p>修改人：Administrator ,  2018年8月28日  下午5:16:30</p>
	 *
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param columnName
	 * @param value
	 * @throws Exception
	 * void 
	 */
	public static void addRow(String tableName,String rowKey,String columnFamily,String columnName,String value) throws Exception {
		HTable hTable = new HTable(conf, TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
		hTable.put(put);
		hTable.close();
	}
	
	
	/**
	 * @Description：获取一行数据
	 * <p>创建人：Administrator ,  2018年8月28日  下午5:25:10</p>
	 * <p>修改人：Administrator ,  2018年8月28日  下午5:25:10</p>
	 *
	 * @param tableName
	 * @param rowKey
	 * @throws Exception
	 * void 
	 */
	public static void getRow(String tableName,String rowKey) throws Exception {
		
		HTable hTable = new HTable(conf, TableName.valueOf(tableName));
		
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = hTable.get(get);
		Cell[] rawCells = result.rawCells();
		for (Cell cell : rawCells) {
			
			System.out.println("行："+Bytes.toString(CellUtil.cloneRow(cell)));
			System.out.println("列族："+Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println("列名："+Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println("值："+Bytes.toString(CellUtil.cloneValue(cell)));
			
		}
		hTable.close();
	}
	
	
	/**
	 * @Description：获取指定列族，列名的数据
	 * <p>创建人：Administrator ,  2018年8月28日  下午5:37:30</p>
	 * <p>修改人：Administrator ,  2018年8月28日  下午5:37:30</p>
	 *
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param columnName
	 * @throws Exception
	 * void 
	 */
	public static void getRowQuality(String tableName,String rowKey,String columnFamily,String columnName) throws Exception {
		HTable hTable = new HTable(conf, TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
		
		Result result = hTable.get(get);
		Cell[] rawCells = result.rawCells();
		for (Cell cell : rawCells) {
			
			System.out.println("行："+Bytes.toString(CellUtil.cloneRow(cell)));
			System.out.println("列族："+Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println("列名："+Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println("值："+Bytes.toString(CellUtil.cloneValue(cell)));
			
		}
		hTable.close();
		
	}
	
	
	/**
	 * @Description：扫描全表，获取所有列
	 * <p>创建人：Administrator ,  2018年8月28日  下午5:53:21</p>
	 * <p>修改人：Administrator ,  2018年8月28日  下午5:53:21</p>
	 *
	 * @param tableName
	 * @throws Exception
	 * void 
	 */
	public static void getAllRow(String tableName) throws Exception {
		HTable hTable = new HTable(conf, TableName.valueOf(tableName));
		Scan scan = new Scan();
		ResultScanner scanner = hTable.getScanner(scan);
		Iterator<Result> iterator = scanner.iterator();
		while(iterator.hasNext()) {
			Result result = iterator.next();
			Cell[] rawCells = result.rawCells();
			for (Cell cell : rawCells) {
				
				System.out.println("行："+Bytes.toString(CellUtil.cloneRow(cell)));
				System.out.println("列族："+Bytes.toString(CellUtil.cloneFamily(cell)));
				System.out.println("列名："+Bytes.toString(CellUtil.cloneQualifier(cell)));
				System.out.println("值："+Bytes.toString(CellUtil.cloneValue(cell)));
				
			}
		}
		
		hTable.close();
		
	} 
}
