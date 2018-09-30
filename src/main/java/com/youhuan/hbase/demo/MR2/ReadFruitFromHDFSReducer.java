package com.youhuan.hbase.demo.MR2;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

public class ReadFruitFromHDFSReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {

	@Override
	protected void reduce(ImmutableBytesWritable key, Iterable<Put> value, Context context)
			throws IOException, InterruptedException {
		
		for (Put put : value) {
			context.write(NullWritable.get(), put);
		}

	}
}
