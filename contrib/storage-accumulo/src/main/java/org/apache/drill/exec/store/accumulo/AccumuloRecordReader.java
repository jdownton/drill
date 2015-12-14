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

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.phemi.agile.util.AgileConf;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class AccumuloRecordReader extends AbstractRecordReader implements org.apache.drill.exec.store.accumulo.DrillAccumuloConstants {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccumuloRecordReader.class);

  private static final int TARGET_RECORD_COUNT = 4000;

  private OutputMutator outputMutator;

  private Map<String, MapVector> familyVectorMap;
  private VarBinaryVector rowKeyVector;

  private DrillAccumuloTable hTable;
  private Scanner resultScanner;

  private Scanner accScanner;
  private Configuration accumuloConf;
  private OperatorContext operatorContext;

  private boolean rowKeyOnly;

  private static Connector conn;

  public AccumuloRecordReader(Configuration conf, AccumuloSubScan.AccumuloSubScanSpec subScanSpec,
                              List<SchemaPath> projectedColumns, FragmentContext context)
          throws OutOfMemoryException, TableNotFoundException {

    accumuloConf = conf;
    accScanner = conn.createScanner(AgileConf.AGILE_TABLE_RAW_DATA, new Authorizations());
    accScanner.setRange(new Range(subScanSpec.getStartRow().toString(), subScanSpec.getStopRow().toString()));

    setColumns(projectedColumns);
  }


  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> columns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    rowKeyOnly = true;
    if (!isStarQuery()) {
      for (SchemaPath column : columns) {
        if (column.getRootSegment().getPath().equalsIgnoreCase(ROW_KEY)) {
          transformed.add(ROW_KEY_PATH);
          continue;
        }
        rowKeyOnly = false;
        NameSegment root = column.getRootSegment();
        byte[] family = root.getPath().getBytes();
        transformed.add(SchemaPath.getSimplePath(root.getPath()));
        PathSegment child = root.getChild();
        if (child != null && child.isNamed()) {
          byte[] qualifier = child.getNameSegment().getPath().getBytes();
          accumuloScan.addColumn(family, qualifier);
        } else {
          accumuloScan.addFamily(family);
        }
      }
      /* if only the row key was requested, add a FirstKeyOnlyFilter to the scan
       * to fetch only one KV from each row. If a filter is already part of this
       * scan, add the FirstKeyOnlyFilter as the LAST filter of a MUST_PASS_ALL
       * FilterList.
       */
      if (rowKeyOnly) {
        accumuloScan.setFilter(
                HBaseUtils.andFilterAtIndex(accumuloScan.getFilter(), HBaseUtils.LAST_FILTER, new FirstKeyOnlyFilter()));
      }
    } else {
      rowKeyOnly = false;
      transformed.add(ROW_KEY_PATH);
    }


    return transformed;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.operatorContext = context;
    this.outputMutator = output;
    familyVectorMap = new HashMap<String, MapVector>();

    try {
      // Add Vectors to output in the order specified when creating reader
      for (SchemaPath column : getColumns()) {
        if (column.equals(ROW_KEY_PATH)) {
          MaterializedField field = MaterializedField.create(column, ROW_KEY_TYPE);
          rowKeyVector = outputMutator.addField(field, VarBinaryVector.class);
        } else {
          getOrCreateFamilyVector(column.getRootSegment().getPath(), false);
        }
      }
      logger.debug("Opening scanner for HBase table '{}', Zookeeper quorum '{}', port '{}', znode '{}'.",
              hbaseTableName, accumuloConf.get(HConstants.ZOOKEEPER_QUORUM),
              accumuloConf.get(HBASE_ZOOKEEPER_PORT), accumuloConf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
      hTable = new HTable(accumuloConf, hbaseTableName);
      resultScanner = hTable.getScanner(accumuloScan);
    } catch (SchemaChangeException | IOException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    Stopwatch watch = new Stopwatch();
    watch.start();
    if (rowKeyVector != null) {
      rowKeyVector.clear();
      rowKeyVector.allocateNew();
    }
    for (ValueVector v : familyVectorMap.values()) {
      v.clear();
      v.allocateNew();
    }

    int rowCount = 0;
    done:
    for (; rowCount < TARGET_RECORD_COUNT; rowCount++) {
      Result result = null;
      try {
        if (operatorContext != null) {
          operatorContext.getStats().startWait();
        }
        try {
          result = resultScanner.next();
        } finally {
          if (operatorContext != null) {
            operatorContext.getStats().stopWait();
          }
        }
      } catch (IOException e) {
        throw new DrillRuntimeException(e);
      }
      if (result == null) {
        break done;
      }

      // parse the result and populate the value vectors
      Cell[] cells = result.rawCells();
      if (rowKeyVector != null) {
        rowKeyVector.getMutator().setSafe(rowCount, cells[0].getRowArray(), cells[0].getRowOffset(), cells[0].getRowLength());
      }
      if (!rowKeyOnly) {
        for (Cell cell : cells) {
          int familyOffset = cell.getFamilyOffset();
          int familyLength = cell.getFamilyLength();
          byte[] familyArray = cell.getFamilyArray();
          MapVector mv = getOrCreateFamilyVector(new String(familyArray, familyOffset, familyLength), true);

          int qualifierOffset = cell.getQualifierOffset();
          int qualifierLength = cell.getQualifierLength();
          byte[] qualifierArray = cell.getQualifierArray();
          NullableVarBinaryVector v = getOrCreateColumnVector(mv, new String(qualifierArray, qualifierOffset, qualifierLength));

          int valueOffset = cell.getValueOffset();
          int valueLength = cell.getValueLength();
          byte[] valueArray = cell.getValueArray();
          v.getMutator().setSafe(rowCount, valueArray, valueOffset, valueLength);
        }
      }
    }

    setOutputRowCount(rowCount);
    logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), rowCount);
    return rowCount;
  }

  private MapVector getOrCreateFamilyVector(String familyName, boolean allocateOnCreate) {
    try {
      MapVector v = familyVectorMap.get(familyName);
      if(v == null) {
        SchemaPath column = SchemaPath.getSimplePath(familyName);
        MaterializedField field = MaterializedField.create(column, COLUMN_FAMILY_TYPE);
        v = outputMutator.addField(field, MapVector.class);
        if (allocateOnCreate) {
          v.allocateNew();
        }
        getColumns().add(column);
        familyVectorMap.put(familyName, v);
      }
      return v;
    } catch (SchemaChangeException e) {
      throw new DrillRuntimeException(e);
    }
  }

  private NullableVarBinaryVector getOrCreateColumnVector(MapVector mv, String qualifier) {
    int oldSize = mv.size();
    NullableVarBinaryVector v = mv.addOrGet(qualifier, COLUMN_TYPE, NullableVarBinaryVector.class);
    if (oldSize != mv.size()) {
      v.allocateNew();
    }
    return v;
  }

  @Override
  public void cleanup() {
    try {
      if (resultScanner != null) {
        resultScanner.close();
      }
      if (hTable != null) {
        hTable.close();
      }
    } catch (IOException e) {
      logger.warn("Failure while closing HBase table: " + hbaseTableName, e);
    }
  }

  private void setOutputRowCount(int count) {
    for (ValueVector vv : familyVectorMap.values()) {
      vv.getMutator().setValueCount(count);
    }
    if (rowKeyVector != null) {
      rowKeyVector.getMutator().setValueCount(count);
    }
  }

}
