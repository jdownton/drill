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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.phemi.agile.util.AgileConf;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

@JsonTypeName(AccumuloStoragePluginConfig.NAME)
public class AccumuloStoragePluginConfig extends StoragePluginConfigBase implements DrillAccumuloConstants {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccumuloStoragePluginConfig.class);

  private Map<String, String> config;

  @JsonIgnore
  private Configuration accumuloConf;

  @JsonIgnore
  private Boolean sizeCalculatorEnabled;

  public static final String NAME = "accumulo";

  @JsonCreator
  public AccumuloStoragePluginConfig(@JsonProperty("config") Map<String, String> props, @JsonProperty("size.calculator.enabled") Boolean sizeCalculatorEnabled) {
    this.config = props;
    if (config == null) {
      config = Maps.newHashMap();
    }
    logger.debug("Initializing Accumulo StoragePlugin configuration with zookeeper quorum '{}', port '{}'.",
        config.get(AgileConf.ZOOKEEPER_HOSTS), "2181");
    if (sizeCalculatorEnabled == null) {
      this.sizeCalculatorEnabled = false;
    } else {
      this.sizeCalculatorEnabled = sizeCalculatorEnabled;
    }
  }

  @JsonProperty
  public Map<String, String> getConfig() {
    return ImmutableMap.copyOf(config);
  }

  @JsonProperty("size.calculator.enabled")
  public boolean isSizeCalculatorEnabled() {
    return sizeCalculatorEnabled;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AccumuloStoragePluginConfig that = (AccumuloStoragePluginConfig) o;
    return config.equals(that.config);
  }

  @Override
  public int hashCode() {
    return this.config != null ? this.config.hashCode() : 0;
  }

  @JsonIgnore
  public Configuration getAccumuloConf() {
    if (accumuloConf == null) {
      accumuloConf = new AgileConf();
      if (config != null) {
        for (Map.Entry<String, String> entry : config.entrySet()) {
          accumuloConf.set(entry.getKey(), entry.getValue());
        }
      }
    }
    return accumuloConf;
  }

  @JsonIgnore
  public String getZookeeperQuorum() {
    return getAccumuloConf().get(AgileConf.ZOOKEEPER_HOSTS);
  }

  @JsonIgnore
  public String getZookeeperport() { return new String("2181"); }

  @JsonIgnore
  @VisibleForTesting
  public void setZookeeperPort(int zookeeperPort) {
    this.config.put(ACCUMULO_ZOOKEEPER_PORT, String.valueOf(zookeeperPort));
    getAccumuloConf().setInt(ACCUMULO_ZOOKEEPER_PORT, zookeeperPort);
  }

}
