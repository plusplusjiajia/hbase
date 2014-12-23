/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowKeyRange;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMultiRowRangeFilter {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final Log LOG = LogFactory.getLog(this.getClass());
  private byte[] family = Bytes.toBytes("family");
  private byte[] qf = Bytes.toBytes("qf");
  private byte[] value = Bytes.toBytes("val");
  private byte[] tableName;

  /**
   * @throws Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  /**
   * @throws Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMultiRowRangeFilterWithRangeOverlap() throws IOException {
    tableName = Bytes.toBytes("testMultiRowRangeFilterWithRangeOverlap");

    HTable ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);

    int numRows = 100;
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowKeyRange> ranges = new ArrayList<RowKeyRange>();
    ranges.add(new RowKeyRange(Bytes.toBytes(10), Bytes.toBytes(20)));
    ranges.add(new RowKeyRange(Bytes.toBytes(15), Bytes.toBytes(40)));
    ranges.add(new RowKeyRange(Bytes.toBytes(65), Bytes.toBytes(75)));
    ranges.add(new RowKeyRange(Bytes.toBytes(60), Bytes.toBytes(70)));
    ranges.add(new RowKeyRange(Bytes.toBytes(60), Bytes.toBytes(80)));

    MultiRowRangeFilter filter = new MultiRowRangeFilter(ranges);
    scan.setFilter(filter);
    ResultScanner scanner = ht.getScanner(scan);
    List<Cell> results = new ArrayList<Cell>();
    Result r;
    while ((r = scanner.next()) != null) {
      for (Cell kv : r.listCells()) {
        results.add(kv);
      }
    }
    LOG.info("found " + results.size() + " results");
    List<Cell> results1 = getScanResult(Bytes.toBytes(10), Bytes.toBytes(40), ht);
    List<Cell> results2 = getScanResult(Bytes.toBytes(60), Bytes.toBytes(80), ht);

    assertEquals(results1.size() + results2.size(), results.size());

    ht.close();
  }

  @Test
  public void testMultiRowRangeFilterWithoutRangeOverlap() throws IOException {
    tableName = Bytes.toBytes("testMultiRowRangeFilterWithoutRangeOverlap");

    HTable ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);

    int numRows = 100;
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowKeyRange> ranges = new ArrayList<RowKeyRange>();
    ranges.add(new RowKeyRange(Bytes.toBytes(30), Bytes.toBytes(40)));
    ranges.add(new RowKeyRange(Bytes.toBytes(10), Bytes.toBytes(20)));
    ranges.add(new RowKeyRange(Bytes.toBytes(60), Bytes.toBytes(70)));

    MultiRowRangeFilter filter = new MultiRowRangeFilter(ranges);
    scan.setFilter(filter);
    ResultScanner scanner = ht.getScanner(scan);
    List<Cell> results = new ArrayList<Cell>();
    Result r;
    while ((r = scanner.next()) != null) {
      for (Cell kv : r.listCells()) {
        results.add(kv);
      }
    }
    LOG.info("found " + results.size() + " results");
    List<Cell> results1 = getScanResult(Bytes.toBytes(10), Bytes.toBytes(20), ht);
    List<Cell> results2 = getScanResult(Bytes.toBytes(30), Bytes.toBytes(40), ht);
    List<Cell> results3 = getScanResult(Bytes.toBytes(60), Bytes.toBytes(70), ht);

    assertEquals(results1.size() + results2.size() + results3.size(), results.size());

    ht.close();
  }

  @Test
  public void testMultiRowRangeWithFilterListAndOperator() throws IOException {
    tableName = Bytes.toBytes("TestMultiRowRangeFilterWithFilterListAndOperator");

    HTable ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);

    int numRows = 100;
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowKeyRange> ranges1 = new ArrayList<RowKeyRange>();
    ranges1.add(new RowKeyRange(Bytes.toBytes(10), Bytes.toBytes(20)));
    ranges1.add(new RowKeyRange(Bytes.toBytes(30), Bytes.toBytes(40)));
    ranges1.add(new RowKeyRange(Bytes.toBytes(60), Bytes.toBytes(70)));

    MultiRowRangeFilter filter1 = new MultiRowRangeFilter(ranges1);

    List<RowKeyRange> ranges2 = new ArrayList<RowKeyRange>();
    ranges2.add(new RowKeyRange(Bytes.toBytes(20), Bytes.toBytes(40)));
    ranges2.add(new RowKeyRange(Bytes.toBytes(80), Bytes.toBytes(90)));

    MultiRowRangeFilter filter2 = new MultiRowRangeFilter(ranges2);

    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    scan.setFilter(filterList);

    ResultScanner scanner = ht.getScanner(scan);
    List<Cell> results = new ArrayList<Cell>();
    Result r;
    while ((r = scanner.next()) != null) {
      for (Cell kv : r.listCells()) {
        results.add(kv);
      }
    }
    LOG.info("found " + results.size() + " results");
    List<Cell> results1 = getScanResult(Bytes.toBytes(30), Bytes.toBytes(40), ht);

    assertEquals(results1.size(), results.size());

    ht.close();
  }

  @Test
  public void testMultiRowRangeWithFilterListOrOperator() throws IOException {
    tableName = Bytes.toBytes("TestMultiRowRangeFilterWithFilterListOrOperator");

    HTable ht = TEST_UTIL.createTable(tableName, family, Integer.MAX_VALUE);

    int numRows = 100;
    generateRows(numRows, ht, family, qf, value);

    Scan scan = new Scan();
    scan.setMaxVersions();

    List<RowKeyRange> ranges1 = new ArrayList<RowKeyRange>();
    // ranges1.add(new RowKeyRange(Bytes.toBytes(10), Bytes.toBytes(20)));
    ranges1.add(new RowKeyRange(Bytes.toBytes(30), Bytes.toBytes(40)));
    ranges1.add(new RowKeyRange(Bytes.toBytes(10), Bytes.toBytes(20)));
    ranges1.add(new RowKeyRange(Bytes.toBytes(60), Bytes.toBytes(70)));

    MultiRowRangeFilter filter1 = new MultiRowRangeFilter(ranges1);

    List<RowKeyRange> ranges2 = new ArrayList<RowKeyRange>();
    ranges2.add(new RowKeyRange(Bytes.toBytes(20), Bytes.toBytes(40)));
    ranges2.add(new RowKeyRange(Bytes.toBytes(80), Bytes.toBytes(90)));

    MultiRowRangeFilter filter2 = new MultiRowRangeFilter(ranges2);

    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    filterList.addFilter(filter1);
    filterList.addFilter(filter2);
    scan.setFilter(filterList);

    ResultScanner scanner = ht.getScanner(scan);
    List<Cell> results = new ArrayList<Cell>();
    Result r;
    while ((r = scanner.next()) != null) {
      for (Cell kv : r.listCells()) {
        results.add(kv);
      }
    }
    LOG.info("found " + results.size() + " results");
    List<Cell> results1 = getScanResult(Bytes.toBytes(10), Bytes.toBytes(40), ht);
    List<Cell> results2 = getScanResult(Bytes.toBytes(60), Bytes.toBytes(70), ht);
    List<Cell> results3 = getScanResult(Bytes.toBytes(80), Bytes.toBytes(90), ht);

    assertEquals(results1.size() + results2.size() + results3.size(), results.size());

    ht.close();
  }

  private void generateRows(int numberOfRows, HTable ht, byte[] family, byte[] qf, byte[] value)
      throws IOException {
    for (int i = 0; i < numberOfRows; i++) {
      byte[] row = Bytes.toBytes(i);
      Put p = new Put(row);
      p.add(family, qf, value);
      ht.put(p);
    }
    TEST_UTIL.flush();
  }

  private List<Cell> getScanResult(byte[] startRow, byte[] stopRow, HTable ht) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions();
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);
    ResultScanner scanner = ht.getScanner(scan);
    List<Cell> kvList = new ArrayList<Cell>();
    Result r;
    while ((r = scanner.next()) != null) {
      for (Cell kv : r.listCells()) {
        kvList.add(kv);
      }
    }
    return kvList;
  }
}