package com.jx.hbase.mapreduce;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Table Mapper的工作原理
 * 当把MapReduce任务使用扫描器扫描结果的时候， 每一行记录都会
 * 调用一次TableMapper的map方法。 在map方法中可以获取该行记录的所
 * 有内容， 处理后把数据存入Context类中。 使用的方法是
 */
public class MyMapper extends TableMapper<Text, IntWritable> {

    Text text = new Text("allIncomes");

    @Override
    // Hbase 的ImmutableBytesWritable类型一般作为RowKey的类型
    protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
        // 当 scan 遍历每一行记录的时候，这个方法都会调用一次

        // 获取 income的 值
        int income = Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("income")));

        // 转成Hadoop的 int值
        IntWritable intWritable = new IntWritable(income);

        // 将拿到的值，写入到 allincomes 这个键
        context.write(text, intWritable);
    }
}
