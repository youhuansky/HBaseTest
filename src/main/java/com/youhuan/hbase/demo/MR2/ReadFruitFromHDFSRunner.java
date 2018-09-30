package com.youhuan.hbase.demo.MR2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReadFruitFromHDFSRunner extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		//实例化job
		Job job = Job.getInstance(conf);
		job.setJarByClass(ReadFruitFromHDFSRunner.class);
		//组装mapper
		job.setMapperClass(ReadFruitFromHDFSMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class );
		job.setMapOutputValueClass(Put.class);
		
		//组装数据输入
		FileInputFormat.addInputPath(job, new Path("/input_fruit"));
		//组装reducer
		TableMapReduceUtil.initTableReducerJob("fruit_mr2", ReadFruitFromHDFSReducer.class, job);
		//设置reducer的个数
		job.setNumReduceTasks(1);
		return job.waitForCompletion(true)?0:1;
	}
	
	
	public static void main(String[] args) {
		Configuration conf = HBaseConfiguration.create();
		try {
			int status=ToolRunner.run(conf, new ReadFruitFromHDFSRunner(), args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
