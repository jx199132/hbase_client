package com.jx.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

public class HBaseClientTest {

    @Test
    public void testCreate() throws URISyntaxException, IOException {

        // 创建配置类
        Configuration configuration = HBaseConfiguration.create();

        // 读取配置文件，初始化配置
        configuration.addResource(
                new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI())
        );

        // 根据配置信息，创建客户端连接
        Connection connection = ConnectionFactory.createConnection(configuration);

        // 定义一个表名为 mytable的 对象
        TableName tableName = TableName.valueOf("mytable");

        // 创建描述信息对象
        HTableDescriptor table = new HTableDescriptor(tableName);

        // 创建一个列族名称
        HColumnDescriptor familyColumn = new HColumnDescriptor("cf");

        // 把这个列族添加到表里面去
        table.addFamily(familyColumn);

        // 获取执行类
        Admin admin = connection.getAdmin();

        // 创建表
        admin.createTable(table);

        admin.close();
        connection.close();


        // 如果出现  客户端一直连接不上，并且  输出信息中又  Will not attempt to authenticate using SASL (unknown error) ，
        // 那么检查一下是否可以连接zookeeper， 很可能就是 zookeeper配置的访问方式和这里不一致。  如果zoo.cfg配置的ip，那么这里就要是ip。如果是域名，那么这里也要是域名
    }

}
