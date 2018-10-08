package com.youhuan.hbase.demo.weibo;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import com.youhuan.hbase.demo.weibo.constants.Constants;

public class Weibo {

	private Configuration conf = HBaseConfiguration.create();

	/**
	 * @Description：创建命名空间，创建三张表
	 *                           <p>
	 *                           创建人：Administrator , 2018年9月13日 下午4:04:43
	 *                           </p>
	 *                           <p>
	 *                           修改人：Administrator , 2018年9月13日 下午4:04:43
	 *                           </p>
	 *
	 *                           void
	 */
	public void init() {
		// initNamespace();
		createTableContent();
	}

	public static void main(String[] args) {
		Weibo weibo = new Weibo();
		weibo.init();
	}

	/**
	 * @throws Exception
	 * @Description：初始化命名空间
	 *                      <p>
	 *                      创建人：Administrator , 2018年9月13日 下午4:05:09
	 *                      </p>
	 *                      <p>
	 *                      修改人：Administrator , 2018年9月13日 下午4:05:09
	 *                      </p>
	 *
	 *                      void
	 */
	public void initNamespace() {
		HBaseAdmin admin = null;

		Connection connection = null;
		try {

			connection = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin) connection.getAdmin();
			// admin = new HBaseAdmin(conf);
			NamespaceDescriptor ns_weibo = NamespaceDescriptor.create(Constants.namespaceName)
					.addConfiguration("creator", "youhuan").build();
			admin.createNamespace(ns_weibo);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	/**
	 * @Description：创建微博内容表
	 *                      <p>
	 *                      创建人：Administrator , 2018年9月13日 下午4:34:38
	 *                      </p>
	 *                      <p>
	 *                      修改人：Administrator , 2018年9月13日 下午4:34:38
	 *                      </p>
	 *
	 *                      void
	 */
	public void createTableContent() {

		HBaseAdmin admin = null;

		Connection connection = null;
		try {

			connection = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin) connection.getAdmin();
			// 创建表描述器
			HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(Constants.TABLE_CONTENT));
			// 创建列描述器
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes(Constants.cf_info));
			// 压缩
//			hColumnDescriptor.setCompressionType(Algorithm.SNAPPY);
			// 设置版本确界
			hColumnDescriptor.setVersions(1, 1);

			hTableDescriptor.addFamily(hColumnDescriptor);

			admin.createTable(hTableDescriptor);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	/**
	 * @Description：创建用户关系表
	 *                      <p>
	 *                      创建人：Administrator , 2018年9月13日 下午5:03:11
	 *                      </p>
	 *                      <p>
	 *                      修改人：Administrator , 2018年9月13日 下午5:03:11
	 *                      </p>
	 *
	 *                      void
	 */
	public void createTableRelation() {

		HBaseAdmin admin = null;

		Connection connection = null;
		try {

			connection = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin) connection.getAdmin();
			// 创建表描述器
			HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(Constants.TABLE_RELATIONS));
			// 创建列描述器
			HColumnDescriptor att_hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes(Constants.cf_attends));
			HColumnDescriptor fans_hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes(Constants.cf_fans));
			// 压缩
			att_hColumnDescriptor.setCompressionType(Algorithm.SNAPPY);
			// 设置版本确界
			att_hColumnDescriptor.setVersions(1, 1);
			// 压缩
			fans_hColumnDescriptor.setCompressionType(Algorithm.SNAPPY);
			// 设置版本确界
			fans_hColumnDescriptor.setVersions(1, 1);

			hTableDescriptor.addFamily(att_hColumnDescriptor);
			hTableDescriptor.addFamily(fans_hColumnDescriptor);

			admin.createTable(hTableDescriptor);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	/**
	 * @Description：创建邮箱表
	 *                    <p>
	 *                    创建人：Administrator , 2018年9月13日 下午5:03:11
	 *                    </p>
	 *                    <p>
	 *                    修改人：Administrator , 2018年9月13日 下午5:03:11
	 *                    </p>
	 *
	 *                    void
	 */
	public void createTableInBox() {

		HBaseAdmin admin = null;

		Connection connection = null;
		try {

			connection = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin) connection.getAdmin();
			// 创建表描述器
			HTableDescriptor hTableDescriptor = new HTableDescriptor(
					TableName.valueOf(Constants.TABLE_RECEIVE_CONTENT_EMAIL));
			// 创建列描述器
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes(Constants.cf_info));
			// 压缩
			hColumnDescriptor.setCompressionType(Algorithm.SNAPPY);
			// 设置版本确界
			hColumnDescriptor.setVersions(1, 1);

			hTableDescriptor.addFamily(hColumnDescriptor);

			admin.createTable(hTableDescriptor);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	public void publishContent(String uid, String content) {

		HBaseAdmin admin = null;

		Connection connection = null;
		try {
			// 1.将发布的微博内容发布到微博内容表中
			connection = ConnectionFactory.createConnection(conf);
			Table contentTable = connection.getTable(TableName.valueOf(Constants.TABLE_CONTENT));
			// 设计rowkey
			String rowKey = uid + System.currentTimeMillis();
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(Constants.cf_info), Bytes.toBytes("content"), Bytes.toBytes(content));
			contentTable.put(put);

			// 2.将发布的微博内容推送到粉丝收件箱中。
			// 2.1先获取到该发布微博粉丝
			Table relationTable = connection.getTable(TableName.valueOf(Constants.TABLE_RELATIONS));
			// 2.2取出所有粉丝数据
			Get relationGet = new Get(Bytes.toBytes(uid));
			relationGet.addFamily(Bytes.toBytes(Constants.cf_fans));
			Result relationResult = relationTable.get(relationGet);
			List<byte[]> arrayList = new ArrayList<byte[]>();
			Cell[] rawCells = relationResult.rawCells();
			for (Cell cell : rawCells) {
				byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
				arrayList.add(cloneQualifier);
			}
			// 如果没有粉丝直接return
			if (arrayList.size() == 0) {
				return;
			}
			// 2.3开始操作收件箱表
			Table inBoxTable = connection.getTable(TableName.valueOf(Constants.TABLE_RECEIVE_CONTENT_EMAIL));
			List<Put> putList = new ArrayList<Put>();

			for (byte[] fans : arrayList) {
				Put fansput = new Put(fans);
				fansput.addColumn(Bytes.toBytes(Constants.cf_info), Bytes.toBytes(uid), Bytes.toBytes(rowKey));
				putList.add(fansput);
			}
			inBoxTable.put(putList);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	/**
	 * @Description：
	 * <p>
	 * 创建人：Administrator , 2018年9月21日 下午4:39:49
	 * </p>
	 * <p>
	 * 修改人：Administrator , 2018年9月21日 下午4:39:49
	 * </p>
	 *
	 * @param uid
	 * @param attends
	 *            void
	 */
	public void addAttends(String uid, String... attends) {

		if (StringUtils.isBlank(uid) || attends.length < 0) {
			return;
		}

		HBaseAdmin admin = null;

		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			// 1.获取我的用户关系表对象
			Table contentTable = connection.getTable(TableName.valueOf(Constants.TABLE_RELATIONS));
			// 定义List集合用于存放发封装put对象
			List<Put> putList = new ArrayList<Put>();
			Put myPut = new Put(Bytes.toBytes(uid));
			for (String attend : attends) {
				myPut.addColumn(Bytes.toBytes(Constants.cf_fans), Bytes.toBytes(attend), Bytes.toBytes(attend));
				Put fansPut = new Put(Bytes.toBytes(attend));
				fansPut.addColumn(Bytes.toBytes(Constants.cf_fans), Bytes.toBytes(uid), Bytes.toBytes(attend));
				putList.add(fansPut);
			}
			putList.add(myPut);
			contentTable.put(putList);

			// 3向我收件箱表中插入我关注人的rowkey
			Table contentTable2 = connection.getTable(TableName.valueOf(Constants.TABLE_CONTENT));
			Scan scan = new Scan();
			// 用于存放关注的人的发过的微博rowkey
			List<byte[]> rowkeysList = new ArrayList<byte[]>();
			// 所有关注用户微博的rowkey
			for (String string_uid : attends) {
				// 实例化rowkey的过滤器
				RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(string_uid + "_"));
				scan.setFilter(rowFilter);
				ResultScanner resultScanner = contentTable2.getScanner(scan);
				for (Result result : resultScanner) {
					Cell[] rawCells = result.rawCells();
					for (Cell cell : rawCells) {
						byte[] cloneRow = CellUtil.cloneRow(cell);
						rowkeysList.add(cloneRow);
					}
				}
				if (rowkeysList.size() == 0) {
					continue;
				}
				Table inboxTable = connection.getTable(TableName.valueOf(Constants.TABLE_RECEIVE_CONTENT_EMAIL));
				List<Put> putsList = new ArrayList<Put>();
				Put p = new Put(Bytes.toBytes(uid));
				for (byte[] rk : rowkeysList) {
					String rowkey = Bytes.toString(rk);
					// String attend_uid=
					p.addColumn(Bytes.toBytes(Constants.cf_info), Bytes.toBytes(string_uid), rk);

				}
				inboxTable.put(p);
				rowkeysList.clear();
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}
				if (connection != null) {
					connection.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

	/**
	 * @Description：移除关注的用户
	 *                      <p>
	 *                      创建人：Administrator , 2018年9月27日 下午5:46:09
	 *                      </p>
	 *                      <p>
	 *                      修改人：Administrator , 2018年9月27日 下午5:46:09
	 *                      </p>
	 *
	 * @param uid
	 * @param attends
	 *            void
	 */
	public void removeAttends(String uid, String... attends) {

		if (StringUtils.isEmpty(uid) || attends.length < 0) {
			return;
		}

		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			// 1.获取我的用户关系表对象
			Table contentTable = connection.getTable(TableName.valueOf(Constants.TABLE_RELATIONS));
			// 待删除的delete集合
			List<Delete> deleteList = new ArrayList<>();
			Delete attendDelete = new Delete(Bytes.toBytes(uid));
			for (String attend : attends) {
				attendDelete.addColumn(Bytes.toBytes(Constants.cf_attends), Bytes.toBytes(attend));
				// 移除对象粉丝列表中的我
				Delete fansDelete = new Delete(Bytes.toBytes(attend));
				fansDelete.addColumn(Bytes.toBytes(Constants.cf_fans), Bytes.toBytes(uid));
				deleteList.add(fansDelete);
			}
			deleteList.add(attendDelete);
			contentTable.delete(deleteList);
			// 删除我的邮件箱中已经移除用户微博的rowkey
			Table indexTable = connection.getTable(TableName.valueOf(Constants.TABLE_RECEIVE_CONTENT_EMAIL));
			// 得到收件箱中对应的我的rowkey的哪一行数据
			Delete inboxDelete = new Delete(Bytes.toBytes(uid));
			for (String attend : attends) {
				inboxDelete.addColumn(Bytes.toBytes(Constants.cf_info), Bytes.toBytes(attend));
			}
			indexTable.delete(inboxDelete);

		} catch (Exception e) {
		}

	}

	public List<Message> getAttendsContent(String uid) {
		List<Message> messageList = new ArrayList<Message>();
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			// 1.获取我的用户关系表对象
			Table inboxTable = connection.getTable(TableName.valueOf(Constants.TABLE_RECEIVE_CONTENT_EMAIL));
			// 待删除的delete集合
			Get get = new Get(Bytes.toBytes(uid));
			// 设置确界
			get.setMaxVersions(50);
			List<byte[]> rowkeyList = new ArrayList<byte[]>();
			Result result = inboxTable.get(get);
			for (Cell cell : result.rawCells()) {
				rowkeyList.add(CellUtil.cloneValue(cell));
			}
			Table contentTable = connection.getTable(TableName.valueOf(Constants.TABLE_RECEIVE_CONTENT_EMAIL));
			List<Get> getList = new ArrayList<Get>();
			for (byte[] rk : rowkeyList) {
				Get rkGet = new Get(rk);
				getList.add(rkGet);
			}
			Result[] results = contentTable.get(getList);
			for (Result r : results) {
				for (Cell c : r.rawCells()) {
					Message message = new Message();
					String rowkey = Bytes.toString(CellUtil.cloneRow(c));
					String u = rowkey.substring(0, rowkey.indexOf("_"));
					String st = rowkey.substring(rowkey.indexOf("_") + 1);
					String content = Bytes.toString(CellUtil.cloneValue(c));

					message.setUid(u);
					message.setTimestamp(st);
					message.setContent(content);
					messageList.add(message);
				}
			}

			return messageList;
		} catch (Exception e) {
		}
		return messageList;
	}

}
