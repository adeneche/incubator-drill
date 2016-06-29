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
package org.apache.drill.exec.ops;

import java.util.List;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.UserBitShared.MinorFragmentProfile;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;

/**
 * Holds statistics of a particular (minor) fragment.
 */
public class FragmentStats {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FragmentStats.class);

  public enum WAIT_TYPE {
    NONE,
    READ,
    SEND
  }

  private List<OperatorStats> operators = Lists.newArrayList();
  private long startTime;
  private final DrillbitEndpoint endpoint;
  private final BufferAllocator allocator;
  private long totalTimeEnqueued;

  private long startWaitTime;
  private WAIT_TYPE waitType = WAIT_TYPE.NONE;

  private long totalWaitOnRead;
  private long totalWaitOnSend;

  private long maxConsecutiveRuntime;
  private long setupTime;

  public FragmentStats(BufferAllocator allocator, MetricRegistry metrics, DrillbitEndpoint endpoint) {
    this.endpoint = endpoint;
    this.allocator = allocator;
  }

  public void addMetricsToStatus(MinorFragmentProfile.Builder prfB) {
    prfB.setStartTime(startTime);
    prfB.setMaxMemoryUsed(allocator.getPeakMemoryAllocation());
    prfB.setEndTime(System.currentTimeMillis());
    prfB.setTotalTimeQueued(totalTimeEnqueued);

    long waitDuration = System.currentTimeMillis() - startWaitTime;
    long waitOnRead = totalWaitOnRead + (waitType == WAIT_TYPE.READ ? waitDuration : 0);
    long waitOnSend = totalWaitOnSend + (waitType == WAIT_TYPE.SEND ? waitDuration : 0);
    prfB.setWaitOnRead(waitOnRead);
    prfB.setWaitOnSend(waitOnSend);
    prfB.setMaxRuntime(maxConsecutiveRuntime);
    prfB.setSetupTime(setupTime);

    prfB.setEndpoint(endpoint);
    for(OperatorStats o : operators){
      prfB.addOperatorProfile(o.getProfile());
    }
  }

  public void setStartTimeToNow() {
    startTime = System.currentTimeMillis();
  }

  public void addTimeInQueue(long timeInQueue) {
    totalTimeEnqueued += timeInQueue;
  }

  public void setConsecutiveRuntime(long duration) {
    if (duration > maxConsecutiveRuntime) {
      maxConsecutiveRuntime = duration;
    }
  }

  public void setSetupTime(long duration) {
    setupTime = duration;
  }

  public void startWait(WAIT_TYPE waitType) {
    assert this.waitType == WAIT_TYPE.NONE : "shouldn't call startWait until previous wait is stopped";

    this.waitType = waitType;
    startWaitTime = System.currentTimeMillis();
  }

  public void stopWait() {
    long duration = System.currentTimeMillis() - startWaitTime;

    switch (waitType) {
      case READ:
        totalWaitOnRead += duration;
        break;
      case SEND:
        totalWaitOnSend += duration;
        break;
    }

    waitType = WAIT_TYPE.NONE;
  }

  /**
   * Creates a new holder for operator statistics within this holder for fragment statistics.
   *
   * @param profileDef operator profile definition
   * @param allocator the allocator being used
   * @return a new operator statistics holder
   */
  public OperatorStats newOperatorStats(final OpProfileDef profileDef, final BufferAllocator allocator) {
    final OperatorStats stats = new OperatorStats(profileDef, allocator);
    if(profileDef.operatorType != -1) {
      operators.add(stats);
    }
    return stats;
  }

  public void addOperatorStats(OperatorStats stats) {
    operators.add(stats);
  }

}
