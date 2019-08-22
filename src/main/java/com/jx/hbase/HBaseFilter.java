package com.jx.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.hbase.filter.MultiRowRangeFilter.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class HBaseFilter {


    Configuration configuration;
    Connection connection;
    Table table;

    @Before
    public void before() {
        try {
            configuration = HBaseConfiguration.create();
            configuration.addResource(
                    new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI())
            );

            connection = ConnectionFactory.createConnection(configuration);
            table = connection.getTable(TableName.valueOf("mymoney"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void putBatch() throws IOException {
        byte[] family = Bytes.toBytes("mycf");
        byte[] qualifier = Bytes.toBytes("name");

        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(family, qualifier, Bytes.toBytes("billyWangpaul"));

        Put put2 = new Put(Bytes.toBytes("row2"));
        put2.addColumn(family, qualifier, Bytes.toBytes("sara"));

        Put put3 = new Put(Bytes.toBytes("row3"));
        put3.addColumn(family, qualifier, Bytes.toBytes("chris"));

        Put put4 = new Put(Bytes.toBytes("row4"));
        put4.addColumn(family, qualifier, Bytes.toBytes("helen"));

        Put put5 = new Put(Bytes.toBytes("row5"));
        put5.addColumn(family, qualifier, Bytes.toBytes("andyWang"));

        Put put6 = new Put(Bytes.toBytes("row6"));
        put6.addColumn(family, qualifier, Bytes.toBytes("kateWang"));

        List<Put> putList = new LinkedList<>();
        putList.add(put1);
        putList.add(put2);
        putList.add(put3);
        putList.add(put4);
        putList.add(put5);
        putList.add(put6);

        table.put(putList);
    }


    @Test
    public void valueFilter() throws IOException {
        Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("Wang"));
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String name = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name")));
            System.out.println(name);
        }
        rs.close();
    }


    @Test
    public void singleColumnValueFilter() throws IOException {
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("mycf"), Bytes.toBytes("name"), CompareFilter.CompareOp.EQUAL, new SubstringComparator("Wang"));
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String name = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name")));
            System.out.println(name);
        }
        rs.close();
    }

    @Test
    public void singleColumnValueFilter2() throws IOException {
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("mycf"), Bytes.toBytes("teacher"), CompareFilter.CompareOp.EQUAL, new SubstringComparator("Wang"));
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String name = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name")));
            System.out.println(name);
        }
        rs.close();
    }

    @Test
    public void singleColumnValueFilter3() throws IOException {

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        // 只有列族为 mycf 的记录才放入结果集
        Filter familyfilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("mycf")));
        filterList.addFilter(familyfilter);

        // 只有列为 teacher 的记录才放入结果集
        Filter colFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("teacher")));
        filterList.addFilter(colFilter);

        // 只有值包含 Wang 的记录才放入结果集
        Filter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("Wang"));
        filterList.addFilter(valueFilter);


        Scan scan = new Scan();
        scan.setFilter(filterList);

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String name = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name")));
            System.out.println(name);
        }
        rs.close();
    }


    @Test
    public void pageFilter() throws IOException {
        Filter filter = new PageFilter(2);

        Scan scan = new Scan();
        scan.setFilter(filter);

        byte[] rowKey = null;

        System.out.println("第一页");
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String name = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name")));
            System.out.println(name);
            rowKey = r.getRow();
        }
        rs.close();

        System.out.println("第二页");

        // 为lastRowkey 拼接一个零字节 ， 这样就可以避免分页的时候包含了上一页的最后一个数据
        rowKey = Bytes.add(rowKey, new byte[1]);

        scan.setStartRow(rowKey);
        rs = table.getScanner(scan);
        for (Result r : rs) {
            String name = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name")));
            System.out.println(name);
            rowKey = r.getRow();
        }
        rs.close();
    }

    @Test
    public void filterList() {
        FilterList filterList = new FilterList();

        Filter ageFilter = new SingleColumnValueFilter(
                Bytes.toBytes("mycf"),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.GREATER,
                new LongComparator(22)
        );

        Filter pageFilter = new PageFilter(2);

        filterList.addFilter(ageFilter);
        filterList.addFilter(pageFilter);

        Scan scan = new Scan();
        scan.setFilter(filterList);
    }

    @Test
    public void filterList2() {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        Filter ageFilter = new SingleColumnValueFilter(
                Bytes.toBytes("mycf"),
                Bytes.toBytes("age"),
                CompareFilter.CompareOp.GREATER,
                new LongComparator(22)
        );

        Filter nameFilter = new SingleColumnValueFilter(
                Bytes.toBytes("mycf"),
                Bytes.toBytes("name"),
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("Wang")
        );

        Filter pageFilter = new PageFilter(2);

        filterList.addFilter(ageFilter);
        filterList.addFilter(nameFilter);

        FilterList filterList2 = new FilterList();
        filterList2.addFilter(filterList);
        filterList.addFilter(pageFilter);

        Scan scan = new Scan();
        scan.setFilter(filterList2);
    }

    @Test
    public void rowFilter() throws IOException {
        Filter filter = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes("row3")));

        Scan scan = new Scan();
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String name = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name")));
            System.out.println(name);
        }
        rs.close();
    }

    @Test
    public void multiRowRangeFilter() throws IOException {
        RowRange r1 = new RowRange("row1", true, "row2", true);
        RowRange r2 = new RowRange("row3", true, "row4", true);

        List<RowRange> rangeList = new LinkedList<>();
        rangeList.add(r1);
        rangeList.add(r2);

        Filter multtiRowRangeFilter = new MultiRowRangeFilter(rangeList);

        Scan scan = new Scan();
        scan.setFilter(multtiRowRangeFilter);

        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String name = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name")));
            System.out.println(name);
        }
        rs.close();
    }

    @Test
    public void prefixFilter() {
        Filter filter = new PrefixFilter(Bytes.toBytes("row"));
    }

    @Test
    public void fuzzyRowFilter() {
        Filter filter = new FuzzyRowFilter(
                Arrays.asList(
                        new Pair<>(
                                Bytes.toBytesBinary("2016_??_??_4567"),
                                new byte[]{0, 0, 0, 0, 0, 1, 1, 0, 1, 1, 0, 0, 0, 0, 0}
                        )
                )
        );

        Scan scan = new Scan();
        scan.setFilter(filter);
    }

    @Test
    public void inclusiveStopFilter() {
        Filter filter = new InclusiveStopFilter(Bytes.toBytes("row5"));
    }

    @Test
    public void randomRowFilter() {
        Filter filter = new RandomRowFilter(0.5f);
    }

    @Test
    public void familyFilter() {
        Filter filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("mycf")));
    }

    @Test
    public void qualifierFilter() {
        Filter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("name")));
    }

    @Test
    public void columnPrefixFilter() {
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes("nam"));
    }

    @Test
    public void multipleColumnPrefixFilter() {
        byte[][] filter_prefix = new byte[2][];
        filter_prefix[0] = Bytes.toBytes("ci");
        filter_prefix[1] = Bytes.toBytes("ac");

        MultipleColumnPrefixFilter filter = new MultipleColumnPrefixFilter(filter_prefix);
    }

    @Test
    public void keyOnlyFilter() throws IOException {
        KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();
        Scan scan = new Scan();
        scan.setFilter(keyOnlyFilter);

        ResultScanner rs = table.getScanner(scan);

        for (Result r : rs) {
            List<Cell> cells = r.listCells();
            List<String> sb = new LinkedList<>();

            byte[] rowkey = r.getRow();
            sb.add("row=" + Bytes.toString(rowkey));
            for (Cell cell : cells) {
                sb.add("column=" + new String(CellUtil.cloneQualifier(cell)));
            }
            System.out.println(StringUtils.join(sb, ", "));
        }
        rs.close();
    }

    @Test
    public void firstKeyOnlyFilter() throws IOException {
        Filter filter = new FirstKeyOnlyFilter();
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner rs = table.getScanner(scan);

        int count = 0;
        for (Result r : rs) {
            count++;
        }
        rs.close();
    }

    @Test
    public void putSchoolBatch() throws IOException {
        byte[] family = Bytes.toBytes("info");
        byte[] qualifier1 = Bytes.toBytes("geo");
        byte[] qualifier2 = Bytes.toBytes("name");

        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(family, qualifier1, Bytes.toBytes("north"))
                .addColumn(family, qualifier2, Bytes.toBytes("qinghua"));

        Put put2 = new Put(Bytes.toBytes("row2"));
        put2.addColumn(family, qualifier1, Bytes.toBytes("north"))
                .addColumn(family, qualifier2, Bytes.toBytes("beijing"));

        Put put3 = new Put(Bytes.toBytes("row3"));
        put3.addColumn(family, qualifier1, Bytes.toBytes("north"))
                .addColumn(family, qualifier2, Bytes.toBytes("xiamen"));

        Put put4 = new Put(Bytes.toBytes("row4"));
        put4.addColumn(family, qualifier1, Bytes.toBytes("south"))
                .addColumn(family, qualifier2, Bytes.toBytes("shenzhen"));

        Put put5 = new Put(Bytes.toBytes("row5"));
        put5.addColumn(family, qualifier1, Bytes.toBytes("south"))
                .addColumn(family, qualifier2, Bytes.toBytes("zhejiang"));


        List<Put> putList = new LinkedList<>();
        putList.add(put1);
        putList.add(put2);
        putList.add(put3);
        putList.add(put4);
        putList.add(put5);

        table.put(putList);
    }


    @Test
    public void skipFilter() throws IOException {
        Filter f1 = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("north")));

        Filter filter = new SkipFilter(f1);

        Scan scan = new Scan();
        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);

        for (Result r : rs) {
            List<Cell> cells = r.listCells();
            List<String> sb = new LinkedList<>();

            byte[] rowkey = r.getRow();
            sb.add("row=" + Bytes.toString(rowkey));
            for (Cell cell : cells) {
                sb.add("column=" + new String(CellUtil.cloneQualifier(cell)));
            }
            System.out.println(StringUtils.join(sb, ", "));
        }
        rs.close();
    }

    @Test
    public void batchPutMymoney() throws IOException {
        byte[] family = Bytes.toBytes("info");
        byte[] q1 = Bytes.toBytes("income");
        byte[] q2 = Bytes.toBytes("expense");

        int[] q1Array = {6000, 6600, 4000, 5310, 4500, 5500, 5600, 4900, 5600, 6900, 5800, 5700};
        int[] q2Array = {5000, 5300, 5200, 5320, 4800, 4500, 5200, 5100, 5200, 5900, 6100, 6000};

        for (int i = 1; i <= 12; i++) {
            byte[] rowkey = null;
            if (i < 10) {
                rowkey = Bytes.toBytes(String.valueOf("0" + i));
            } else {
                rowkey = Bytes.toBytes(String.valueOf(i));
            }
            Put put = new Put(rowkey);
            put.addColumn(family, q1, Bytes.toBytes(q1Array[i - 1]))
                    .addColumn(family, q2, Bytes.toBytes(q2Array[i - 1]));

            table.put(put);
        }
    }

    @Test
    public void scan() throws IOException {
        ResultScanner rs = table.getScanner(new Scan());
        for (Result r : rs) {
            List<Cell> cells = r.listCells();
            List<String> sb = new LinkedList<>();

            byte[] rowkey = r.getRow();
            sb.add("row=" + Bytes.toString(rowkey));
            for (Cell cell : cells) {
                sb.add("column=" + new String(CellUtil.cloneQualifier(cell)));
                sb.add(":" + Bytes.toInt(CellUtil.cloneValue(cell)));
            }
            System.out.println(StringUtils.join(sb, ", "));
        }
        rs.close();
    }
}
