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

import com.fasterxml.jackson.annotation.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

// Class containing information for reading a single HBase region
@JsonTypeName("accumulo-tserver-scan")
public class AccumuloSubScan extends AbstractBase implements SubScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(org.apache.drill.exec.store.accumulo.AccumuloSubScan.class);

  @JsonProperty
  public final AccumuloStoragePluginConfig storage;

  @JsonIgnore
  private final AccumuloStoragePlugin accumuloStoragePlugin;
  private final List<AccumuloSubScanSpec> regionScanSpecList;
  private final List<SchemaPath> columns;

  @JsonCreator
  public AccumuloSubScan(@JacksonInject StoragePluginRegistry registry,
                         @JsonProperty("userName") String userName,
                         @JsonProperty("storage") StoragePluginConfig storage,
                         @JsonProperty("regionScanSpecList") LinkedList<AccumuloSubScanSpec> regionScanSpecList,
                         @JsonProperty("columns") List<SchemaPath> columns) throws ExecutionSetupException {
    super(userName);
    accumuloStoragePlugin = (AccumuloStoragePlugin) registry.getPlugin(storage);
    this.regionScanSpecList = regionScanSpecList;
    this.storage = (AccumuloStoragePluginConfig) storage;
    this.columns = columns;
  }

  public AccumuloSubScan(String userName, AccumuloStoragePlugin plugin, AccumuloStoragePluginConfig config,
                         List<AccumuloSubScanSpec> regionInfoList, List<SchemaPath> columns) {
    super(userName);
    accumuloStoragePlugin = plugin;
    storage = config;
    this.regionScanSpecList = regionInfoList;
    this.columns = columns;
  }

  public List<AccumuloSubScanSpec> getRegionScanSpecList() {
    return regionScanSpecList;
  }

  @JsonIgnore
  public AccumuloStoragePluginConfig getStorageConfig() {
    return storage;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @JsonIgnore
  public AccumuloStoragePlugin getStorageEngine(){
    return accumuloStoragePlugin;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new org.apache.drill.exec.store.accumulo.AccumuloSubScan(getUserName(), accumuloStoragePlugin, storage, regionScanSpecList, columns);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  public static class AccumuloSubScanSpec {

    protected String tableName;
    protected String tabletServer;
    protected byte[] startRow;
    protected byte[] stopRow;
    protected byte[] serializedFilter;

    @parquet.org.codehaus.jackson.annotate.JsonCreator
    public AccumuloSubScanSpec(@JsonProperty("tableName") String tableName,
                            @JsonProperty("tabletServer") String tabletServer,
                            @JsonProperty("startRow") byte[] startRow,
                            @JsonProperty("stopRow") byte[] stopRow,
                            @JsonProperty("serializedFilter") byte[] serializedFilter,
                            @JsonProperty("filterString") String filterString) {
      if (serializedFilter != null && filterString != null) {
        throw new IllegalArgumentException("The parameters 'serializedFilter' or 'filterString' cannot be specified at the same time.");
      }
      this.tableName = tableName;
      this.tabletServer = tabletServer;
      this.startRow = startRow;
      this.stopRow = stopRow;
      if (serializedFilter != null) {
        this.serializedFilter = serializedFilter;
      } else {
        this.serializedFilter = AccumuloUtils.serializeFilter(AccumuloUtils.parseFilterString(filterString));
      }
    }

    /* package */ AccumuloSubScanSpec() {
      // empty constructor, to be used with builder pattern;
    }

    public String getTableName() {
      return tableName;
    }

    public AccumuloSubScanSpec setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public String getRegionServer() {
      return tabletServer;
    }

    public AccumuloSubScanSpec setRegionServer(String regionServer) {
      this.tabletServer = regionServer;
      return this;
    }

    public byte[] getStartRow() {
      return startRow;
    }

    public AccumuloSubScanSpec setStartRow(byte[] startRow) {
      this.startRow = startRow;
      return this;
    }

    public byte[] getStopRow() {
      return stopRow;
    }

    public AccumuloSubScanSpec setStopRow(byte[] stopRow) {
      this.stopRow = stopRow;
      return this;
    }

    public byte[] getSerializedFilter() {
      return serializedFilter;
    }

    @Override
    public String toString() {
      return "AccumuloScanSpec [tableName=" + tableName
          + ", startRow=" + (startRow == null ? null : new String(startRow))
          + ", stopRow=" + (stopRow == null ? null : new String(stopRow))
          + ", tabletServer=" + tabletServer + "]";
    }

  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.HBASE_SUB_SCAN_VALUE;
  }

}
