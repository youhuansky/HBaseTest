package com.youhuan.hbase.demo.weibo.constants;

import org.apache.hadoop.hbase.util.Bytes;

public class Constants {
	
	// 命名空间名字
	public static String namespaceName="ns_weibo";
	// 列族名称
	public static String cf_info="info";
	// 列族名称
	public static String cf_attends="attends";
	// 列族名称
	public static String cf_fans="fans";
	// 微博内容表的表名
	public static final byte[] TABLE_CONTENT = Bytes.toBytes(namespaceName+":content");
	// 用户关系表的表名
	public static final byte[] TABLE_RELATIONS = Bytes.toBytes(namespaceName+":relations");
	// 微博收件箱表的表名
	public static final byte[] TABLE_RECEIVE_CONTENT_EMAIL = Bytes.toBytes(namespaceName+":receive_content_email");

}
