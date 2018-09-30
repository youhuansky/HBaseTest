package com.youhuan.hbase.demo.MR2;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReadFruitFromHDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {
		
		String string = value.toString();
		String[] split = string.split("\t");
		Put put = new Put(Bytes.toBytes(split[0]));
		put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(split[1]));
		put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(split[2]));
		
		ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes(split[0]));
		context.write(immutableBytesWritable, put);
	}
}
