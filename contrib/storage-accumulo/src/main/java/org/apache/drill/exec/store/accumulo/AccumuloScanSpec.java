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


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.accumulo.core.iterators.Filter;

public class AccumuloScanSpec {

  protected String tableName;
  protected byte[] startRow;
  protected byte[] stopRow;

  protected Filter filter;
  protected String filterString;

  @JsonCreator
  public AccumuloScanSpec(@JsonProperty("tableName") String tableName,
                          @JsonProperty("startRow") byte[] startRow,
                          @JsonProperty("stopRow") byte[] stopRow,
                          @JsonProperty("filterString") String filterString) {
    this.tableName = tableName;
    this.startRow = startRow;
    this.stopRow = stopRow;

    // TODO: change this to a proper filter once scanning works
    if (filterString != null) {
      this.filter = null;
      this.filterString = filterString;
    }
  }

  public AccumuloScanSpec(String tableName, byte[] startRow, byte[] stopRow, Filter filter) {
    this.tableName = tableName;
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.filter = filter;
  }

  public AccumuloScanSpec(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  public byte[] getStartRow() {
    return startRow == null ? null : startRow;
  }

  public byte[] getStopRow() {
    return stopRow == null ? null : stopRow;
  }

  @Override
  public String toString() {
    return "HBaseScanSpec [tableName=" + tableName
        + ", startRow=" + (startRow == null ? null : new String(startRow))
        + ", stopRow=" + (stopRow == null ? null :  new String(stopRow))
        + ", filter=" + (filter == null ? null : filterString)
        + "]";
  }

}
