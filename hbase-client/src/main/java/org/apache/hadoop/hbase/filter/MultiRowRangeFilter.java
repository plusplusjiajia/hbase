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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.HBaseZeroCopyByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Filter to support scan multiple row key ranges. It can construct the row key ranges from the
 * passed list which can be accessed by each region server.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MultiRowRangeFilter extends FilterBase {

  private List<RowKeyRange> rangeList;

  private boolean done = false;
  private boolean initialized = false;
  private int index;
  private RowKeyRange range;
  private ReturnCode currentReturnCode;

  public MultiRowRangeFilter() {
  }

  /**
   * @param list
   *          A list of <code>RowKeyRange</code>
   * @throws java.io.IOException
   *           throw an exception if the range list is not in an natural order or any
   *           <code>RowKeyRange</code> is invalid
   */
  public MultiRowRangeFilter(List<RowKeyRange> list) throws IOException {
    this.rangeList = sortAndMerge(list);
  }

  @Override
  public boolean filterAllRemaining() {
    return done;
  }

  public List<RowKeyRange> getRowRanges() {
    return this.rangeList;
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    // If it is the first time of running, calculate the current range index for
    // the row key. If index is out of bound which happens when the start row
    // user set is after the largest stop row of the ranges, stop the scan.
    // If row key is after the current range, find the next range and update index.
    if (!initialized || !range.contains(buffer, offset, length)) {
      byte[] rowkey = new byte[length];
      System.arraycopy(buffer, offset, rowkey, 0, length);
      index = getNextRangeIndex(rowkey);
      if (index >= rangeList.size()) {
        done = true;
        currentReturnCode = ReturnCode.NEXT_ROW;
        return false;
      }
      range = rangeList.get(index);
      if (!initialized) {
        if (range.contains(buffer, offset, length)) {
          currentReturnCode = ReturnCode.INCLUDE;
        } else {
          currentReturnCode = ReturnCode.SEEK_NEXT_USING_HINT;
        }
      } else {
        currentReturnCode = ReturnCode.SEEK_NEXT_USING_HINT;
      }
      initialized = true;
    } else {
      currentReturnCode = ReturnCode.INCLUDE;
    }
    return false;
  }

  @Override
  public ReturnCode filterKeyValue(Cell ignored) {
    return currentReturnCode;
  }

  @Override
  public Cell getNextCellHint(Cell currentKV) {
    return KeyValueUtil.createFirstOnRow(range.startRow);
  }

  /**
   * @return The filter serialized using pb
   */
  public byte[] toByteArray() {
    FilterProtos.MultiRowRangeFilter.Builder builder = FilterProtos.MultiRowRangeFilter
        .newBuilder();
    for (RowKeyRange range : rangeList) {
      if (range != null) {
        FilterProtos.RowKeyRange.Builder rangebuilder = FilterProtos.RowKeyRange.newBuilder();
        if (range.startRow != null)
          rangebuilder.setStartRow(HBaseZeroCopyByteString.wrap(range.startRow));
        if (range.stopRow != null)
          rangebuilder.setStopRow(HBaseZeroCopyByteString.wrap(range.stopRow));
        range.isScan = Bytes.equals(range.startRow, range.stopRow) ? 1 : 0;
        builder.addRowKeyRangeList(rangebuilder.build());
      }
    }
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes
   *          A pb serialized instance
   * @return An instance of MultiRowRangeFilter
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   */
  public static MultiRowRangeFilter parseFrom(final byte[] pbBytes)
      throws DeserializationException {
    FilterProtos.MultiRowRangeFilter proto;
    try {
      proto = FilterProtos.MultiRowRangeFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    int length = proto.getRowKeyRangeListCount();
    List<FilterProtos.RowKeyRange> rangeProtos = proto.getRowKeyRangeListList();
    List<RowKeyRange> rangeList = new ArrayList<RowKeyRange>(length);
    for (FilterProtos.RowKeyRange rangeProto : rangeProtos) {
      RowKeyRange range = new RowKeyRange(rangeProto.hasStartRow() ? rangeProto.getStartRow()
          .toByteArray() : null, rangeProto.hasStopRow() ? rangeProto.getStopRow().toByteArray()
          : null);
      rangeList.add(range);
    }
    try {
      return new MultiRowRangeFilter(rangeList);
    } catch (IOException e) {
      throw new DeserializationException("Fail to instantiate the MultiRowRangeFilter", e);
    }
  }

  /**
   * @param o
   *          the filter to compare
   * @return true if and only if the fields of the filter that are serialized are equal to the
   *         corresponding fields in other. Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this)
      return true;
    if (!(o instanceof MultiRowRangeFilter))
      return false;

    MultiRowRangeFilter other = (MultiRowRangeFilter) o;
    if (this.rangeList.size() != other.rangeList.size())
      return false;
    for (int i = 0; i < rangeList.size(); ++i) {
      RowKeyRange thisRange = this.rangeList.get(i);
      RowKeyRange otherRange = other.rangeList.get(i);
      if (!(Bytes.equals(thisRange.startRow, otherRange.startRow) && Bytes.equals(
          thisRange.stopRow, otherRange.stopRow))) {
        return false;
      }
    }
    return true;
  }

  /**
   * calculate the position where the row key in the ranges list.
   *
   * @param rowKey
   *          the row key to calculate
   * @return index the position of the row key
   */
  private int getNextRangeIndex(byte[] rowKey) {
    RowKeyRange temp = new RowKeyRange(rowKey, null);
    int index = Collections.binarySearch(rangeList, temp);
    if (index < 0) {
      int insertionPosition = -index - 1;
      // check if the row key in the range before the insertion position
      if (insertionPosition != 0 && rangeList.get(insertionPosition - 1).contains(rowKey))
        return insertionPosition - 1;

      return insertionPosition;
    }
    // equals one of the start keys
    return index;
  }

  /**
   * sort the ranges and if the ranges with overlap, then merge them.
   *
   * @param ranges
   *          the list of range to sort and merge.
   *
   */
  public static List<RowKeyRange> sortAndMerge(List<RowKeyRange> ranges) throws IOException {
    if (ranges.size() == 0) {
      throw new IllegalArgumentException("No ranges found.");
    }
    List<RowKeyRange> invalidRanges = new ArrayList<RowKeyRange>();
    List<RowKeyRange> newRanges = new ArrayList<RowKeyRange>(ranges.size());
    Collections.sort(ranges);
    if(ranges.get(0).isValid()) {
      if (ranges.size() == 1) {
        newRanges.add(ranges.get(0));
      }
    } else {
      invalidRanges.add(ranges.get(0));
    }
    byte[] lastStartRow = ranges.get(0).startRow;
    byte[] lastStopRow = ranges.get(0).stopRow;
    int i = 1;
    for (; i < ranges.size(); i++) {
      RowKeyRange range = ranges.get(i);
      if (!range.isValid()) {
        invalidRanges.add(range);
      }
      if(Bytes.equals(lastStopRow, HConstants.EMPTY_BYTE_ARRAY)) {
        newRanges.add(new RowKeyRange(lastStartRow, lastStopRow));
        break;
      }
      // with overlap in the ranges
      if (Bytes.compareTo(lastStopRow, range.startRow) >= 0) {
        if(Bytes.equals(range.stopRow, HConstants.EMPTY_BYTE_ARRAY)) {
          newRanges.add(new RowKeyRange(lastStartRow, range.stopRow));
          break;
        }
        // if first range contains second range, ignore the second range
        if (Bytes.compareTo(lastStopRow, range.stopRow) >= 0) {
          if ((i + 1) == ranges.size()) {
            newRanges.add(new RowKeyRange(lastStartRow, lastStopRow));
          }
        } else {
          lastStopRow = range.stopRow;
          if ((i + 1) < ranges.size()) {
            i++;
            range = ranges.get(i);
            if (!range.isValid()) {
              invalidRanges.add(range);
            }
          } else {
            newRanges.add(new RowKeyRange(lastStartRow, lastStopRow));
            break;
          }
          while (Bytes.compareTo(lastStopRow, range.startRow) >= 0) {
            if(Bytes.equals(range.stopRow, HConstants.EMPTY_BYTE_ARRAY)) {
              break;
            }
            // if this first range contain second range, ignore the second range
            if (Bytes.compareTo(lastStopRow, range.stopRow) >= 0) {
              i++;
              if (i < ranges.size()) {
                range = ranges.get(i);
                if (!range.isValid()) {
                  invalidRanges.add(range);
                }
              } else {
                break;
              }
            } else {
              lastStopRow = range.stopRow;
              i++;
              if (i < ranges.size()) {
                range = ranges.get(i);
                if (!range.isValid()) {
                  invalidRanges.add(range);
                }
              } else {
                break;
              }
            }
          }
          if(Bytes.equals(range.stopRow, HConstants.EMPTY_BYTE_ARRAY)) {
            if(Bytes.compareTo(lastStopRow, range.startRow) < 0)
            {
              newRanges.add(new RowKeyRange(lastStartRow, lastStopRow));
              newRanges.add(range);
            } else {
              newRanges.add(new RowKeyRange(lastStartRow, range.stopRow));
              break;
            }
          }
          newRanges.add(new RowKeyRange(lastStartRow, lastStopRow));
          lastStartRow = range.startRow;
          lastStopRow = range.stopRow;
          if ((i + 1) == ranges.size()) {
            newRanges.add(new RowKeyRange(lastStartRow, lastStopRow));
          }
        }
      } else {
        newRanges.add(new RowKeyRange(lastStartRow, lastStopRow));
        lastStartRow = range.startRow;
        lastStopRow = range.stopRow;
        if ((i + 1) == ranges.size()) {
          newRanges.add(new RowKeyRange(lastStartRow, lastStopRow));
        }
      }
    }
    // check the remaining ranges
    for(int j=i; j < ranges.size(); j++) {
      if(!ranges.get(j).isValid()) {
        invalidRanges.add(ranges.get(j));
      }
    }
    // if invalid range exists, throw the exception
    if (invalidRanges.size() != 0) {
      printInvalidRanges(invalidRanges, true);
    }
    // If no valid ranges found, throw the exception
    if(newRanges.size() == 0) {
      throw new IllegalArgumentException("No valid ranges found.");
    }
    return newRanges;
  }

  private static void printInvalidRanges(List<RowKeyRange> invalidRanges, boolean details) {
      StringBuilder sb = new StringBuilder();
      sb.append(invalidRanges.size()).append(" invaild ranges.\n");
      if (details) {
        for (RowKeyRange range : invalidRanges) {
          sb.append(
              "Invalid range: start row => " + Bytes.toString(range.startRow) + ", stop row => "
                  + Bytes.toString(range.stopRow)).append('\n');
        }
      }
      throw new IllegalArgumentException(sb.toString());
  }

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class RowKeyRange implements Comparable<RowKeyRange> {
    private byte[] startRow;
    private byte[] stopRow;
    private int isScan = 0;

    public RowKeyRange() {
    }

    /**
     * if the startRow is empty or null, set it to HConstants.EMPTY_BYTE_ARRAY, means begin at the
     * start row of the table if the stopRow is empty or null, set it to
     * HConstants.EMPTY_BYTE_ARRAY, means end of the last row of table
     */
    public RowKeyRange(String startRow, String stopRow) {
      this((startRow == null || startRow.isEmpty()) ? HConstants.EMPTY_BYTE_ARRAY : Bytes
          .toBytes(startRow), (stopRow == null || stopRow.isEmpty()) ? HConstants.EMPTY_BYTE_ARRAY
          : Bytes.toBytes(stopRow));
    }

    public RowKeyRange(byte[] startRow, byte[] stopRow) {
      this.startRow = (startRow == null) ? HConstants.EMPTY_BYTE_ARRAY : startRow;
      this.stopRow = (stopRow == null) ? HConstants.EMPTY_BYTE_ARRAY :stopRow;
      isScan = Bytes.equals(startRow, stopRow) ? 1 : 0;
    }

    public byte[] getStartRow() {
      return startRow;
    }

    public byte[] getStopRow() {
      return stopRow;
    }

    public boolean contains(byte[] row) {
      return contains(row, 0, row.length);
    }

    public boolean contains(byte[] buffer, int offset, int length) {
      return Bytes.compareTo(buffer, offset, length, startRow, 0, startRow.length) >= 0
          && (Bytes.equals(stopRow, HConstants.EMPTY_BYTE_ARRAY) || Bytes.compareTo(buffer, offset,
              length, stopRow, 0, stopRow.length) < isScan);
    }

    @Override
    public int compareTo(RowKeyRange other) {
      return Bytes.compareTo(startRow, other.startRow);
    }

    public boolean isValid() {
      return Bytes.equals(startRow, HConstants.EMPTY_BYTE_ARRAY)
          || Bytes.equals(stopRow, HConstants.EMPTY_BYTE_ARRAY)
          || Bytes.compareTo(startRow, stopRow) <= 0;
    }
  }
}
