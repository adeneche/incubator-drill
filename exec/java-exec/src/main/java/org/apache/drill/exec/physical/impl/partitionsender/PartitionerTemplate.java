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
package org.apache.drill.exec.physical.impl.partitionsender;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nullable;
import javax.inject.Named;

import com.google.common.base.Function;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.compile.sig.RuntimeOverridden;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.AccountingDataTunnel;
import org.apache.drill.exec.ops.DelegatingAccountingDataTunnel;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.MinorFragmentEndpoint;
import org.apache.drill.exec.physical.config.HashPartitionSender;
import org.apache.drill.exec.physical.impl.partitionsender.PartitionSenderRootExec.Metric;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Lists;
import org.apache.parquet.Preconditions;

public abstract class PartitionerTemplate implements Partitioner {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionerTemplate.class);

  // Always keep the recordCount as (2^x) - 1 to better utilize the memory allocation in ValueVectors
  private static final int DEFAULT_RECORD_BATCH_SIZE = (1 << 10) - 1;

  private SelectionVector2 sv2;
  private SelectionVector4 sv4;
  private RecordBatch incoming;
  private OperatorStats stats;
  private int start;
  private int end;
  private List<OutgoingRecordBatch> outgoingBatches = Lists.newArrayList();

  private int outgoingRecordBatchSize = DEFAULT_RECORD_BATCH_SIZE;

  public PartitionerTemplate() { }

  @Override
  public List<? extends PartitionOutgoingBatch> getOutgoingBatches() {
    return outgoingBatches;
  }

  @Override
  public PartitionOutgoingBatch getOutgoingBatch(int index) {
    if ( index >= start && index < end) {
      return outgoingBatches.get(index - start);
    }
    return null;
  }

  @Override
  public void setup(FragmentContext context, RecordBatch incoming, HashPartitionSender popConfig,
                          OperatorStats stats, OperatorContext oContext, int start, int end,
                          final RpcOutcomeListener<GeneralRPCProtos.Ack> sendAvailabilityNotifier) {
    this.incoming = incoming;
    this.stats = stats;
    this.start = start;
    this.end = end;
    doSetup(context, incoming, null);

    // Half the outgoing record batch size if the number of senders exceeds 1000 to reduce the total amount of memory
    // allocated.
    if (popConfig.getDestinations().size() > 1000) {
      // Always keep the recordCount as (2^x) - 1 to better utilize the memory allocation in ValueVectors
      outgoingRecordBatchSize = (DEFAULT_RECORD_BATCH_SIZE + 1)/2 - 1;
    }

    int fieldId = 0;
    logger.debug("initializing outgoing batches. start: {}, end: {}", start, end);
    for (MinorFragmentEndpoint destination : popConfig.getDestinations()) {
      // create outgoingBatches only for subset of Destination Points
      if ( fieldId >= start && fieldId < end ) {
        final AccountingDataTunnel tunnel = DelegatingAccountingDataTunnel.of(context.getDataTunnel(destination), sendAvailabilityNotifier);
        outgoingBatches.add(new OutgoingRecordBatch(stats, popConfig, tunnel, context, oContext.getAllocator()));
      }
      fieldId++;
    }

    // do not initialize before receiving ok_new_schema
    if (incoming.getSchema() == null) {
      return;
    }

    SelectionVectorMode svMode = incoming.getSchema().getSelectionVectorMode();
    switch(svMode){
      case FOUR_BYTE:
        this.sv4 = incoming.getSelectionVector4();
        break;

      case TWO_BYTE:
        this.sv2 = incoming.getSelectionVector2();
        break;

      case NONE:
        break;

      default:
        throw new UnsupportedOperationException("Unknown selection vector mode: " + svMode.toString());
    }
  }

  @Override
  public void initialize() {
    for (final OutgoingRecordBatch batch : outgoingBatches) {
      batch.initialize();
    }
  }

  @Override
  public OperatorStats getStats() {
    return stats;
  }

  /**
   * Flush each outgoing record batch, and optionally reset the state of each outgoing record
   * batch (on schema change).  Note that the schema is updated based on incoming at the time
   * this function is invoked.
   *
   * @param isLastBatch    true if this is the last incoming batch
   * @param isSchemaChanged  true if the schema has changed
   */
  @Override
  public boolean flushOutgoingBatches(boolean isLastBatch, boolean isSchemaChanged) {
    boolean allFlushed = true;
    for (final OutgoingRecordBatch batch : outgoingBatches) {
      final boolean isFlushed = batch.flush(isLastBatch);
      logger.debug("flushing batch: {} -- flushed: {}", batch.getTunnel().getRemoteEndpoint(), isFlushed);

      if (isFlushed || isSchemaChanged) {
        batch.clear();
        if (!isLastBatch) {
          batch.initialize();
        }
      }
      allFlushed = allFlushed && isFlushed;
    }
    logger.debug("Attempted to flush all outgoing batches. total: {} allFlushed: {}", outgoingBatches.size(), allFlushed);
    return allFlushed;
  }

  public boolean send(final FragmentWritableBatch batch) {
    boolean allSent = true;
    for (final OutgoingRecordBatch outgoing:outgoingBatches) {
      final boolean sent = outgoing.send(batch);
      allSent = allSent && sent;
    }
    return allSent;
  }

  @Override
  public boolean flushIfReady() {
    boolean allFlushed = true;
    for (final OutgoingRecordBatch batch : outgoingBatches) {
      final boolean isFlushed = batch.flushIfBatchIsFilled();
      if (isFlushed) {
        batch.clear();
        batch.initialize();
      }
      allFlushed = allFlushed && isFlushed;
    }
    return allFlushed;
  }

  @Override
  public void partitionBatch(RecordBatch incoming) {
    SelectionVectorMode svMode = incoming.getSchema().getSelectionVectorMode();

    // Keeping the for loop inside the case to avoid case evaluation for each record.
    switch(svMode) {
      case NONE:
        for (int recordId = 0; recordId < incoming.getRecordCount(); ++recordId) {
          doCopy(recordId);
        }
        break;

      case TWO_BYTE:
        for (int recordId = 0; recordId < incoming.getRecordCount(); ++recordId) {
          int svIndex = sv2.getIndex(recordId);
          doCopy(svIndex);
        }
        break;

      case FOUR_BYTE:
        for (int recordId = 0; recordId < incoming.getRecordCount(); ++recordId) {
          int svIndex = sv4.get(recordId);
          doCopy(svIndex);
        }
        break;

      default:
        throw new UnsupportedOperationException("Unknown selection vector mode: " + svMode.toString());
    }
  }

  /**
   * Helper method to copy data based on partition
   * @param svIndex
   * @throws IOException
   */
  private void doCopy(int svIndex) {
    int index = doEval(svIndex);
    if ( index >= start && index < end) {
      OutgoingRecordBatch outgoingBatch = outgoingBatches.get(index - start);
      outgoingBatch.copy(svIndex);
    }
  }

  @Override
  public void clear() {
    for (OutgoingRecordBatch outgoingRecordBatch : outgoingBatches) {
      outgoingRecordBatch.clear();
    }
  }

  @Override
  public boolean canSend() {
    for (final OutgoingRecordBatch batch:outgoingBatches) {
      if (!batch.tunnel.isSendingBufferAvailable()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public AccountingDataTunnel[] getTunnels() {
    final List<AccountingDataTunnel> tunnels = Lists.transform(outgoingBatches,
        new Function<OutgoingRecordBatch, AccountingDataTunnel>() {
          @Nullable
          @Override
          public AccountingDataTunnel apply(final OutgoingRecordBatch input) {
            return Preconditions.checkNotNull(input, "outgoing batch is required").tunnel;
          }
        });

    return (AccountingDataTunnel[])tunnels.toArray();
  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") OutgoingRecordBatch[] outgoing);
  public abstract int doEval(@Named("inIndex") int inIndex);

  public class OutgoingRecordBatch implements PartitionOutgoingBatch, VectorAccessible {

    private final AccountingDataTunnel tunnel;
    private final HashPartitionSender operator;
    private final FragmentContext context;
    private final BufferAllocator allocator;
    private final VectorContainer vectorContainer = new VectorContainer();
    private final OperatorStats stats;

    private volatile boolean dropAll = false;
    private int recordCount;
    private int totalRecords;

    public OutgoingRecordBatch(OperatorStats stats, HashPartitionSender operator, AccountingDataTunnel tunnel,
                               FragmentContext context, BufferAllocator allocator) {
      this.context = context;
      this.allocator = allocator;
      this.operator = operator;
      this.tunnel = tunnel;
      this.stats = stats;
    }

    protected void copy(int inIndex) {
      doEval(inIndex, recordCount);
      recordCount++;
      totalRecords++;
    }

    @Override
    public void terminate() {
      // receiver already terminated, don't send anything to it from now on
      dropAll = true;
    }

    @RuntimeOverridden
    protected void doSetup(@Named("incoming") RecordBatch incoming, @Named("outgoing") VectorAccessible outgoing) {};

    @RuntimeOverridden
    protected void doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex) { };

    public boolean flushIfBatchIsFilled() {
      if (recordCount == outgoingRecordBatchSize) {
        return flush(false);
      }
      return true;
    }

    public boolean send(final FragmentWritableBatch batch) {
      stats.startWait();
      try {
        if (!tunnel.isSendingBufferAvailable()) {
          return false;
        }
        tunnel.sendRecordBatch(batch);
        updateStats(batch);
      } finally {
        stats.stopWait();
      }
      return true;
    }

    public boolean flush(final boolean isLastBatch) {
      logger.debug("flushing outgoing batch. isLast: {} -- recordCount: {}", isLastBatch, recordCount);

      if (dropAll) {
        logger.debug("this batch is terminated. dropping all records");

        // If we are in dropAll mode, we still want to copy the data, because we can't stop copying a single outgoing
        // batch with out stopping all outgoing batches. Other option is check for status of dropAll before copying
        // every single record in copy method which has the overhead for every record all the time. Resetting the output
        // count, reusing the same buffers and copying has overhead only for outgoing batches whose receiver has
        // terminated.

        // Reset the count to 0 and use existing buffers for exhausting input where receiver of this batch is terminated
        recordCount = 0;
        // allow sending last batch even after termination
        if (!isLastBatch) {
          return true;
        }
      }
      final FragmentHandle handle = context.getHandle();

      // We need to send the last batch when
      //   1. we are actually done processing the incoming RecordBatches and no more input available
      //   2. receiver wants to terminate (possible in case of queries involving limit clause). Even when receiver wants
      //      to terminate we need to send at least one batch with "isLastBatch" set to true, so that receiver knows
      //      sender has acknowledged the terminate request. After sending the last batch, all further batches are
      //      dropped.

      // if the batch is not the last batch and the current recordCount is zero, then no need to send any RecordBatches
      if (!isLastBatch && recordCount == 0) {
        return true;
      }

      if (recordCount != 0) {
        for (VectorWrapper<?> w : vectorContainer) {
          w.getValueVector().getMutator().setValueCount(recordCount);
        }
      }

      FragmentWritableBatch writableBatch = new FragmentWritableBatch(isLastBatch,
          handle.getQueryId(),
          handle.getMajorFragmentId(),
          handle.getMinorFragmentId(),
          operator.getOppositeMajorFragmentId(),
          tunnel.getRemoteEndpoint().getId(), // opposite minor fragment id
          getWritableBatch());

      if (!send(writableBatch)) {
        return false;
      }

      recordCount = 0;

      // If the current batch is the last batch, then set a flag to ignore any requests to flush the data
      // This is possible when the receiver is terminated, but we still get data from input operator
      if (isLastBatch) {
        dropAll = true;
      }

      return true;
    }

    protected void allocateOutgoingRecordBatch() {
      for (VectorWrapper<?> v : vectorContainer) {
        v.getValueVector().allocateNew();
      }
    }

    public void updateStats(FragmentWritableBatch writableBatch) {
      stats.addLongStat(Metric.BYTES_SENT, writableBatch.getByteCount());
      stats.addLongStat(Metric.BATCHES_SENT, 1);
      stats.addLongStat(Metric.RECORDS_SENT, writableBatch.getHeader().getDef().getRecordCount());
    }

    /**
     * Initialize the OutgoingBatch based on the current schema in incoming RecordBatch
     */
    public void initialize() {
      for (VectorWrapper<?> v : incoming) {
        // create new vector
        ValueVector outgoingVector = TypeHelper.getNewVector(v.getField(), allocator);
        outgoingVector.setInitialCapacity(outgoingRecordBatchSize);
        vectorContainer.add(outgoingVector);
      }
      allocateOutgoingRecordBatch();
      doSetup(incoming, vectorContainer);
    }

    @Override
    public BatchSchema getSchema() {
      return incoming.getSchema();
    }

    @Override
    public int getRecordCount() {
      return recordCount;
    }


    @Override
    public long getTotalRecords() {
      return totalRecords;
    }

    @Override
    public TypedFieldId getValueVectorId(SchemaPath path) {
      return vectorContainer.getValueVectorId(path);
    }

    @Override
    public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... fieldIds) {
      return vectorContainer.getValueAccessorById(clazz, fieldIds);
    }

    @Override
    public Iterator<VectorWrapper<?>> iterator() {
      return vectorContainer.iterator();
    }

    @Override
    public SelectionVector2 getSelectionVector2() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SelectionVector4 getSelectionVector4() {
      throw new UnsupportedOperationException();
    }

    public WritableBatch getWritableBatch() {
      return WritableBatch.getBatchNoHVWrap(recordCount, this, false);
    }

    @Override
    public void clear() {
      recordCount = 0;
      vectorContainer.clear();
    }

    @Override
    public AccountingDataTunnel getTunnel() {
      return tunnel;
    }
  }
}
