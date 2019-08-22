package com.jx.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class HBaseCRUD {

    Configuration configuration;
    Connection connection;
    Table table;

    @Before
    public void before(){
        try {
            configuration = HBaseConfiguration.create();
            configuration.addResource(
                    new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI())
            );

            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf("mytable"));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @After
    public void after(){
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void put() throws IOException {
        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        table.put(put);

        /**
         * hbase(main):002:0> scan 'mytable'
         * ROW                              COLUMN+CELL
         *  row1                            column=cf:name, timestamp=1566267091943, value=zhangsan
         */

        put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("age"),Bytes.toBytes(19L))
           .addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("address"),Bytes.toBytes("wh"));

        table.put(put);
    }


    @Test
    public void checkAndPut() throws IOException {
        byte[] rowId = Bytes.toBytes("row1");
        byte[] family = Bytes.toBytes("cf");
        byte[] qualifier = Bytes.toBytes("name");
        byte[] oldValue = Bytes.toBytes("zhangsan");

        Put put = new Put(rowId);
        put.addColumn(family, qualifier, Bytes.toBytes("lishi"));

        // 原子级操作，查询的结果与传入参数一样，才执行修改
        table.checkAndPut(rowId, family, qualifier, oldValue, put);
    }

    @Test
    public void append() throws IOException {
        byte[] rowId = Bytes.toBytes("row1");
        byte[] family = Bytes.toBytes("cf");
        byte[] qualifier = Bytes.toBytes("name");

        Append append = new Append(rowId);

        // 以前的值是 lishi， 现在将值 后面做追加操作  最终结果是   lishiabc
        append.add(family, qualifier, Bytes.toBytes("abc"));
        table.append(append);
    }

    @Test
    public void incremetn() throws IOException {
        byte[] rowId = Bytes.toBytes("row1");
        byte[] family = Bytes.toBytes("cf2");
        byte[] qualifier = Bytes.toBytes("age");

        // 对 age 进行加 1 的原子操作
        Increment increment = new Increment(rowId);
        increment.addColumn(family, qualifier, 1);
        table.increment(increment);
    }

    @Test
    public void get() throws IOException {
        byte[] rowId = Bytes.toBytes("row1");

        Get get = new Get(rowId);

        Result result = table.get(get);
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            byte[] value = CellUtil.cloneValue(cell);
            byte[] quailifier = CellUtil.cloneQualifier(cell);
            System.out.println(Bytes.toString(quailifier) + " : " + Bytes.toString(value));
        }
    }

    @Test
    public void exists() throws IOException {
        byte[] rowId = Bytes.toBytes("row1");

        Get get = new Get(rowId);

        boolean exists = table.exists(get);
        System.out.println(exists);
    }

    @Test
    public void delete() throws IOException {
        byte[] rowId = Bytes.toBytes("row1");
        Delete delete = new Delete(rowId);
        table.delete(delete);
    }

    @Test
    public void checkAndDelete() throws IOException {
        byte[] rowId = Bytes.toBytes("row1");
        byte[] family = Bytes.toBytes("cf");
        byte[] qualifier = Bytes.toBytes("name");
        byte[] oldValue = Bytes.toBytes("zhangsan");

        Delete delete = new Delete(rowId);

        table.checkAndDelete(rowId, family, qualifier, oldValue, delete);
    }


    @Test
    public void mutation() throws IOException {
        // 想在一行中添加一列的时候同时删除另一列, 构建一个Put来新增列然后新建一个Delete对象来删除另一列， 这两个操作要分两步执行，
        // 而且这两步肯定不属于一个原子操作， 这样既麻烦又危险，在Hbase中可以使用 Mutation 来完成这样的操作

        byte[] rowId = Bytes.toBytes("row1");
        byte[] family = Bytes.toBytes("cf");
        byte[] qualifier = Bytes.toBytes("name");

        RowMutations rowMutations = new RowMutations(rowId);

        Put put = new Put(rowId);
        put.addColumn(family,Bytes.toBytes("city"), Bytes.toBytes("wer"));

        Delete delete = new Delete(rowId);
        delete.addColumn(family, qualifier);

        rowMutations.add(delete);
        rowMutations.add(put);

        table.mutateRow(rowMutations);

        // 同样还有 table.checkAndMutate
    }
}
