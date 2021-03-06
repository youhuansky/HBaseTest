package com.youhuan.hbase.demo.MR1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReadFruitRunner extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		//实例化job
		Job job = Job.getInstance(conf);
		job.setJarByClass(ReadFruitRunner.class);
		//组装mapper
		//设置扫描过程中一次缓存多少数据
		Scan scan = new Scan();
		scan.setCaching(300);
		TableMapReduceUtil.initTableMapperJob(
				"fruit", 
				scan, 
				ReadFruitMapper.class, 
				ImmutableBytesWritable.class, 
				Put.class, 
				job);
		//组装reducer
		TableMapReduceUtil.initTableReducerJob("fruit_mr", ReadFruitReducer.class, job);
		//设置reducer的个数
		job.setNumReduceTasks(1);
		return job.waitForCompletion(true)?0:1;
	}
	
	
	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		try {
			int status=ToolRunner.run(conf, new ReadFruitRunner(), args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
