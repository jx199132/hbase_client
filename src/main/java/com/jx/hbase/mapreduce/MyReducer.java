package com.jx.hbase.mapreduce;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MyReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 每个键都会调用一次 Reduce 操作

        int sum = 0;
        for (IntWritable val : values){
            sum += val.get();
        }
        byte[] rowkey = Bytes.toBytes("total");
        Put put = new Put(rowkey);
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("totalIncome"), Bytes.toBytes(sum));

        // 将put写入到 context里面， 随后这个 Put会自动被执行
        context.write(null, put);
    }
}
