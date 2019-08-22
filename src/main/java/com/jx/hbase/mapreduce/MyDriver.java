package com.jx.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class MyDriver {

    public static void main(String[] args) {

        try {
            // 1 由于MapReduce任务是在服务器执行的，所以不需要配置 config
            Configuration configuration =  HBaseConfiguration.create();
            Job job = Job.getInstance(configuration);

            // 2 设置jar加载路径
            job.setJarByClass(MyDriver.class);

            // 3 设置map和reduce类
            // job.setMapperClass(MyMapper.class);
            // job.setReducerClass(MyReducer.class);


            // 4 设置 HBase任务相关
            Scan scan = new Scan();
            scan.setCaching(50);
            scan.setCacheBlocks(false);

            TableMapReduceUtil.initTableMapperJob(
                    "mymoney",        //输入的表名
                            scan,
                            MyMapper.class,         // Mapper类
                            Text.class,             // Mapper输出的key类型
                            IntWritable.class,      // Mapper输出的value类型
                            job
            );

            TableMapReduceUtil.initTableReducerJob(
                    "mymoney",      // 输出的表
                            MyReducer.class,    // Reducer 类
                            job
            );

            // 5 提交
            boolean result = job.waitForCompletion(true);

            System.exit(result ? 0 : 1);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
