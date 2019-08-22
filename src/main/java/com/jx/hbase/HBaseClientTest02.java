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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HBaseClientTest02 {

    Configuration configuration;
    Connection connection;
    Admin admin;

    @Before
    public void before(){
        try {
            configuration = HBaseConfiguration.create();
            configuration.addResource(
                    new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI())
            );

            connection = ConnectionFactory.createConnection(configuration);

            admin = connection.getAdmin();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @After
    public void after(){
        try {
            admin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 友好的创建方式
     */
    @Test
    public void createOrOverwrite(){
        // 定义一个表名为 mytable的 对象
        TableName tableName = TableName.valueOf("mytable");

        // 创建描述信息对象
        HTableDescriptor table = new HTableDescriptor(tableName);

        // 创建一个列族名称
        HColumnDescriptor familyColumn = new HColumnDescriptor("cf");

        // 把这个列族添加到表里面去
        table.addFamily(familyColumn);

        try {
            if (admin.tableExists(tableName)){
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            admin.createTable(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 添加一个列族
     */
    @Test
    public void addFamilyColumn(){
        TableName tableName = TableName.valueOf("mytable");

        // 创建一个列族名称
        HColumnDescriptor familyColumn = new HColumnDescriptor("cf2");

        try {
            admin.addColumn(tableName, familyColumn);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
