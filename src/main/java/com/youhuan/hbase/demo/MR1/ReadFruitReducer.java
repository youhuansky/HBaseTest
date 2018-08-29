package com.youhuan.hbase.demo.MR1;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ReadFruitReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable>{
	
	
	
	@Override
	protected void reduce(ImmutableBytesWritable key, Iterable<Put> puts,
			Reducer<ImmutableBytesWritable, Put, NullWritable, Mutation>.Context context)
			throws IOException, InterruptedException {

		for (Put put : puts) {
			context.write(NullWritable.get(), put);
		}
	
	}

}
