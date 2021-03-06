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
package org.apache.drill.exec.store;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;

import com.google.common.collect.ImmutableSet;

/** Abstract class for StorePlugin implementations.
 * See StoragePlugin for description of the interface intent and its methods.
 */
public abstract class AbstractStoragePlugin implements StoragePlugin{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractStoragePlugin.class);

  protected AbstractStoragePlugin(){
  }

  @Override
  public boolean supportsRead() {
    return false;
  }

  @Override
  public boolean supportsWrite() {
    return false;
  }

  /**
   * @deprecated Marking for deprecation in next major version release.
   * Use {@link #getPhysicalOptimizerRules(org.apache.drill.exec.ops.OptimizerRulesContext)}
   */
  @Override
  @Deprecated
  public Set<? extends RelOptRule> getOptimizerRules(OptimizerRulesContext optimizerContext) {
    return ImmutableSet.of();
  }

  /** An implementation of this method will return one or more specialized rules that Drill query
   *  optimizer can leverage in <i>logical</i> space. Otherwise, it should return an empty set.
   * @return an empty set or a set of plugin specific logical optimizer rules.
   *
   * Note: Move this method to {@link StoragePlugin} interface in next major version release.
   */
  public Set<? extends RelOptRule> getLogicalOptimizerRules(OptimizerRulesContext optimizerContext) {
    return ImmutableSet.of();
  }

  /** An implementation of this method will return one or more specialized rules that Drill query
   *  optimizer can leverage in <i>physical</i> space. Otherwise, it should return an empty set.
   * @return an empty set or a set of plugin specific physical optimizer rules.
   *
   * Note: Move this method to {@link StoragePlugin} interface in next major version release.
   */
  public Set<? extends RelOptRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
    // To be backward compatible, by default call the getOptimizerRules() method.
    return getOptimizerRules(optimizerRulesContext);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
    return getPhysicalScan(userName, selection, AbstractGroupScan.ALL_COLUMNS);
  }

  @Override
  public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void start() throws IOException {
  }

  @Override
  public void close() throws Exception {
  }

}
