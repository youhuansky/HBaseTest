package com.youhuan.hbase.demo.MR1;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadFruitMapper extends TableMapper<ImmutableBytesWritable, Put>{

	
	/**
	 *(非 Javadoc) 
	 * <p>Title: map</p> 
	 * <p>Description: ImmutableBytesWritable存的是行键</p> 
	 * @param key
	 * @param value
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context) 
	 */ 
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Context context)
			throws IOException, InterruptedException {
		
		Put put = new Put(key.get());
		for (Cell cell : value.rawCells()) {
			if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
				//拿到info的数据
				if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					put.add(cell);
				}else if("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					put.add(cell);
				}else {
					put.add(cell);
				}
				
			}
			
		}
		context.write(key, put);
		
	}
	
}
