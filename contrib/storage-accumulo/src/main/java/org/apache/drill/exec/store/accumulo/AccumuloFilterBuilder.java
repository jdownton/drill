/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.accumulo;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.iterators.user.RowFilter;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.accumulo.core.iterators.Filter;

import java.util.Arrays;

public class AccumuloFilterBuilder extends AbstractExprVisitor<AccumuloScanSpec, Void, RuntimeException> implements org.apache.drill.exec.store.accumulo.DrillAccumuloConstants {

  final private org.apache.drill.exec.store.accumulo.AccumuloGroupScan groupScan;

  final private LogicalExpression le;

  private boolean allExpressionsConverted = true;

  private static Boolean nullComparatorSupported = null;

  AccumuloFilterBuilder(org.apache.drill.exec.store.accumulo.AccumuloGroupScan groupScan, LogicalExpression le) {
    this.groupScan = groupScan;
    this.le = le;
  }

  public AccumuloScanSpec parseTree() {
    AccumuloScanSpec parsedSpec = le.accept(this, null);
    if (parsedSpec != null) {
      parsedSpec = mergeScanSpecs("booleanAnd", this.groupScan.getAccumuloScanSpec(), parsedSpec);
      /*
       * If RowFilter is THE filter attached to the scan specification,
       * remove it since its effect is also achieved through startRow and stopRow.
       */
//      if (parsedSpec.filter instanceof RowFilter &&
//          ((RowFilter)parsedSpec.filter).getComparator() instanceof BinaryComparator) {
//        parsedSpec.filter = null;
//      }
    }
    return parsedSpec;
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  @Override
  public AccumuloScanSpec visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    allExpressionsConverted = false;
    return null;
  }

  @Override
  public AccumuloScanSpec visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
    return visitFunctionCall(op, value);
  }

  @Override
  public AccumuloScanSpec visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    AccumuloScanSpec nodeScanSpec = null;
    String functionName = call.getName();
    ImmutableList<LogicalExpression> args = call.args;

    if (CompareFunctionsProcessor.isCompareFunction(functionName)) {
      /*
       * HBASE-10848: Bug in HBase versions (0.94.[0-18], 0.96.[0-2], 0.98.[0-1])
       * causes a filter with NullComparator to fail. Enable only if specified in
       * the configuration (after ensuring that the HBase cluster has the fix).
       */
      if (nullComparatorSupported == null) {
        nullComparatorSupported = groupScan.getAccumuloConf().getBoolean("drill.accumulo.supports.null.comparator", false);
      }

      CompareFunctionsProcessor processor = CompareFunctionsProcessor.process(call, nullComparatorSupported);
      if (processor.isSuccess()) {
        nodeScanSpec = createAccumuloScanSpec(call, processor);
      }
    } else {
      switch (functionName) {
      case "booleanAnd":
      case "booleanOr":
        AccumuloScanSpec firstScanSpec = args.get(0).accept(this, null);
        for (int i = 1; i < args.size(); ++i) {
          AccumuloScanSpec nextScanSpec = args.get(i).accept(this, null);
          if (firstScanSpec != null && nextScanSpec != null) {
            nodeScanSpec = mergeScanSpecs(functionName, firstScanSpec, nextScanSpec);
          } else {
            allExpressionsConverted = false;
            if ("booleanAnd".equals(functionName)) {
              nodeScanSpec = firstScanSpec == null ? nextScanSpec : firstScanSpec;
            }
          }
          firstScanSpec = nodeScanSpec;
        }
        break;
      }
    }

    if (nodeScanSpec == null) {
      allExpressionsConverted = false;
    }

    return nodeScanSpec;
  }

  private AccumuloScanSpec mergeScanSpecs(String functionName, AccumuloScanSpec leftScanSpec, AccumuloScanSpec rightScanSpec) {
    Filter newFilter = null;
    byte[] startRow = DrillAccumuloConstants.EMPTY_START_ROW.getBytes();
    byte[] stopRow = DrillAccumuloConstants.EMPTY_END_ROW.getBytes();

    switch (functionName) {
    case "booleanAnd":
      newFilter = AccumuloUtils.andFilterAtIndex(leftScanSpec.filter, AccumuloUtils.LAST_FILTER, rightScanSpec.filter);
      startRow = AccumuloUtils.maxOfStartRows(leftScanSpec.startRow, rightScanSpec.startRow);
      stopRow = AccumuloUtils.minOfStopRows(leftScanSpec.stopRow, rightScanSpec.stopRow);
      break;
    case "booleanOr":
      newFilter = AccumuloUtils.orFilterAtIndex(leftScanSpec.filter, AccumuloUtils.LAST_FILTER, rightScanSpec.filter);
      startRow = AccumuloUtils.minOfStartRows(leftScanSpec.startRow, rightScanSpec.startRow);
      stopRow = AccumuloUtils.maxOfStopRows(leftScanSpec.stopRow, rightScanSpec.stopRow);
    }
    return new AccumuloScanSpec(groupScan.getTableName(), startRow, stopRow, newFilter);
  }

  private AccumuloScanSpec createAccumuloScanSpec(FunctionCall call, CompareFunctionsProcessor processor) {
    String functionName = processor.getFunctionName();
    SchemaPath field = processor.getPath();
    byte[] fieldValue = processor.getValue();
    boolean sortOrderAscending = processor.isSortOrderAscending();
    boolean isRowKey = field.getAsUnescapedPath().equals(ROW_KEY);
    if (!(isRowKey
        || (!field.getRootSegment().isLastPath()
            && field.getRootSegment().getChild().isLastPath()
            && field.getRootSegment().getChild().isNamed())
           )
        ) {
      /*
       * if the field in this function is neither the row_key nor a qualified Accumulo column, return.
       */
      return null;
    }

    if (processor.isRowKeyPrefixComparison()) {
      return createRowKeyPrefixScanSpec(call, processor);
    }

    CompareOp compareOp = null;
    boolean isNullTest = false;
    ByteArrayComparable comparator = new BinaryComparator(fieldValue);
    byte[] startRow = DrillAccumuloConstants.EMPTY_START_ROW.getBytes();
    byte[] stopRow = DrillAccumuloConstants.EMPTY_END_ROW.getBytes();
    switch (functionName) {
    case "equal":
      compareOp = CompareOp.EQUAL;
      if (isRowKey) {
        startRow = fieldValue;
        /* stopRow should be just greater than 'value'*/
        stopRow = Arrays.copyOf(fieldValue, fieldValue.length+1);
        compareOp = CompareOp.EQUAL;
      }
      break;
    case "not_equal":
      compareOp = CompareOp.NOT_EQUAL;
      break;
    case "greater_than_or_equal_to":
      if (sortOrderAscending) {
        compareOp = CompareOp.GREATER_OR_EQUAL;
        if (isRowKey) {
          startRow = fieldValue;
        }
      } else {
        compareOp = CompareOp.LESS_OR_EQUAL;
        if (isRowKey) {
          // stopRow should be just greater than 'value'
          stopRow = Arrays.copyOf(fieldValue, fieldValue.length+1);
        }
      }
      break;
    case "greater_than":
      if (sortOrderAscending) {
        compareOp = CompareOp.GREATER;
        if (isRowKey) {
          // startRow should be just greater than 'value'
          startRow = Arrays.copyOf(fieldValue, fieldValue.length+1);
        }
      } else {
        compareOp = CompareOp.LESS;
        if (isRowKey) {
          stopRow = fieldValue;
        }
      }
      break;
    case "less_than_or_equal_to":
      if (sortOrderAscending) {
        compareOp = CompareOp.LESS_OR_EQUAL;
        if (isRowKey) {
          // stopRow should be just greater than 'value'
          stopRow = Arrays.copyOf(fieldValue, fieldValue.length+1);
        }
      } else {
        compareOp = CompareOp.GREATER_OR_EQUAL;
        if (isRowKey) {
          startRow = fieldValue;
        }
      }
      break;
    case "less_than":
      if (sortOrderAscending) {
        compareOp = CompareOp.LESS;
        if (isRowKey) {
          stopRow = fieldValue;
        }
      } else {
        compareOp = CompareOp.GREATER;
        if (isRowKey) {
          // startRow should be just greater than 'value'
          startRow = Arrays.copyOf(fieldValue, fieldValue.length+1);
        }
      }
      break;
    case "isnull":
    case "isNull":
    case "is null":
      if (isRowKey) {
        return null;
      }
      isNullTest = true;
      compareOp = CompareOp.EQUAL;
      comparator = new NullComparator();
      break;
    case "isnotnull":
    case "isNotNull":
    case "is not null":
      if (isRowKey) {
        return null;
      }
      compareOp = CompareOp.NOT_EQUAL;
      comparator = new NullComparator();
      break;
    case "like":
      /*
       * Convert the LIKE operand to Regular Expression pattern so that we can
       * apply RegexStringComparator()
       */
      AccumuloRegexParser parser = new AccumuloRegexParser(call).parse();
      compareOp = CompareOp.EQUAL;
      comparator = new RegexStringComparator(parser.getRegexString());

      /*
       * We can possibly do better if the LIKE operator is on the row_key
       */
      if (isRowKey) {
        String prefix = parser.getPrefixString();
        if (prefix != null) { // group 3 is literal
          /*
           * If there is a literal prefix, it can help us prune the scan to a sub range
           */
          if (prefix.equals(parser.getLikeString())) {
            /* The operand value is literal. This turns the LIKE operator to EQUAL operator */
            startRow = stopRow = fieldValue;
            compareOp = null;
          } else {
            startRow = prefix.getBytes(Charsets.UTF_8);
            stopRow = startRow.clone();
            boolean isMaxVal = true;
            for (int i = stopRow.length - 1; i >= 0 ; --i) {
              int nextByteValue = (0xff & stopRow[i]) + 1;
              if (nextByteValue < 0xff) {
                stopRow[i] = (byte) nextByteValue;
                isMaxVal = false;
                break;
              } else {
                stopRow[i] = 0;
              }
            }
            if (isMaxVal) {
              stopRow = DrillAccumuloConstants.EMPTY_END_ROW.getBytes();
            }
          }
        }
      }
      break;
    }

    if (compareOp != null || startRow != DrillAccumuloConstants.EMPTY_START_ROW.getBytes() || stopRow != DrillAccumuloConstants.EMPTY_END_ROW.getBytes()) {
      Filter filter = null;
      if (isRowKey) {
        if (compareOp != null) {
          filter = new RowFilter(compareOp, comparator);
        }
      } else {
        byte[] family = AccumuloUtils.getBytes(field.getRootSegment().getPath());
        byte[] qualifier = AccumuloUtils.getBytes(field.getRootSegment().getChild().getNameSegment().getPath());
        filter = new SingleColumnValueFilter(family, qualifier, compareOp, comparator);
        ((SingleColumnValueFilter)filter).setLatestVersionOnly(true);
        if (!isNullTest) {
          ((SingleColumnValueFilter)filter).setFilterIfMissing(true);
        }
      }
      return new AccumuloScanSpec(groupScan.getTableName(), startRow, stopRow, filter);
    }
    // else
    return null;
  }

private AccumuloScanSpec createRowKeyPrefixScanSpec(FunctionCall call,
    CompareFunctionsProcessor processor) {
    byte[] startRow = processor.getRowKeyPrefixStartRow();
    byte[] stopRow  = processor.getRowKeyPrefixStopRow();
    Filter filter   = processor.getRowKeyPrefixFilter();

    if (startRow != DrillAccumuloConstants.EMPTY_START_ROW.getBytes() ||
      stopRow != DrillAccumuloConstants.EMPTY_END_ROW.getBytes() ||
      filter != null) {
      return new AccumuloScanSpec(groupScan.getTableName(), startRow, stopRow, filter);
    }

    // else
    return null;
}

}
