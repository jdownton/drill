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

import com.phemi.agile.util.AgileConf;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.proxy.thrift;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;


public class AccumuloSchemaFactory implements SchemaFactory {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccumuloSchemaFactory.class);

  final String schemaName;
  final org.apache.drill.exec.store.accumulo.AccumuloStoragePlugin plugin;

  public AccumuloSchemaFactory(AccumuloStoragePlugin plugin, String name) throws IOException {
    this.plugin = plugin;
    this.schemaName = name;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    AccumuloSchema schema = new AccumuloSchema(schemaName);
    SchemaPlus hPlus = parent.add(schemaName, schema);
    schema.setHolder(hPlus);
  }

  class AccumuloSchema extends AbstractSchema {

    public AccumuloSchema(String name) {
      super(ImmutableList.<String>of(), name);
    }

    public void setHolder(SchemaPlus plusOfThis) {
    }

    @Override
    public AbstractSchema getSubSchema(String name) {
      return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return Collections.emptySet();
    }

    @Override
    public Table getTable(String name) {
      AccumuloScanSpec scanSpec = new AccumuloScanSpec(name);
      return new org.apache.drill.exec.store.accumulo.DrillAccumuloTable(schemaName, plugin, scanSpec);
    }

    @Override
    public Set<String> getTableNames() {

      try {
        AgileConf agileConf = (AgileConf)getConf();
        Connector conn = agileConf.newAccumuloConnector();
        Set<String> tableNames = conn.tableOperations().list();
        return tableNames;
      } catch (Exception e) {
        logger.warn("Failure while loading table names for database '{}'.", schemaName, e.getCause());
        return Collections.emptySet();
      }

//      try(AccumuloAdmin admin = new AccumuloAdmin(plugin.getConfig().getAccumuloConf())) {
//        AccumuloTableDescriptor[] tables = admin.listTables();
//        Set<String> tableNames = Sets.newHashSet();
//        for (AccumuloTableDescriptor table : tables) {
//          tableNames.add(new String(table.getName()));
//        }
//        return tableNames;
//      } catch (Exception e) {
//        logger.warn("Failure while loading table names for database '{}'.", schemaName, e.getCause());
//        return Collections.emptySet();
//      }

      // eventually we'll return a list of tables
      return Collections.emptySet();

    }

    @Override
    public String getTypeName() {
      return AccumuloStoragePluginConfig.NAME;
    }

  }

}
