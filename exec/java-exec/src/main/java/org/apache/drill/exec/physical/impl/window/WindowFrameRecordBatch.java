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
package org.apache.drill.exec.physical.impl.window;

import java.io.IOException;
import java.util.List;

import com.sun.codemodel.JExpression;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JVar;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.logical.data.Order;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.expr.fn.FunctionLookupContext;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.WindowPOP;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;

/**
 * support for OVER(PARTITION BY expression1,expression2,... [ORDER BY expressionA, expressionB,...])
 *
 * Doesn't support distinct partitions: multiple window with different PARTITION BY clauses.
 */
public class WindowFrameRecordBatch extends AbstractRecordBatch<WindowPOP> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WindowFrameRecordBatch.class);

  private final RecordBatch incoming;
  private List<WindowDataBatch> batches;
  private WindowFramer framer;

  private boolean noMoreBatches;
  private BatchSchema schema;

  public WindowFrameRecordBatch(WindowPOP popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context);
    this.incoming = incoming;
    batches = Lists.newArrayList();
  }

  /**
   * Let's assume we have the following 3 batches of data:
   * <p><pre>
   * +---------+--------+--------------+--------+
   * |   b0    |   b1   |      b2      |   b3   |
   * +----+----+--------+----+----+----+--------+
   * | p0 | p1 |   p1   | p2 | p3 | p4 |   p5   |
   * +----+----+--------+----+----+----+--------+
   * </pre></p>
   *
   * batch b0 contains partitions p0 and p1
   * batch b1 contains partition p1
   * batch b2 contains partitions p2 p3 and p4
   * batch b3 contains partition p5
   *
   * <p><pre>
   * when innerNext() is called:
   *   call next(incoming), we receive and save b0 in a list of WindowDataBatch
   *     we can't process b0 yet because we don't know if p1 has more rows upstream
   *   call next(incoming), we receive and save b1
   *     we can't process b0 yet for the same reason previously stated
   *   call next(incoming), we receive and save b2
   *   we process b0 (using the framer) and pass the container downstream
   * when innerNext() is called:
   *   we process b1 and pass the container downstream, b0 and b1 are released from memory
   * when innerNext() is called:
   *   call next(incoming), we receive and save b3
   *   we process b2 and pass the container downstream, b2 is released from memory
   * when innerNext() is called:
   *   call next(incoming) and receive NONE
   *   we process b3 and pass the container downstream, b3 is released from memory
   * when innerNext() is called:
   *  we return NONE
   * </pre></p>
   * The previous scenario applies when we don't have an ORDER BY clause, otherwise a batch can be processed
   * as soon as we reach the final peer row of the batch's last row (we find the end of the last frame of the batch).
   * </p>
   * Because we only support the default frame, we don't need to reset the aggregations until we reach the end of
   * a partition. We can safely free a batch as soon as it has been processed.
   */
  @Override
  public IterOutcome innerNext() {
    logger.trace("innerNext(), noMoreBatches = {}", noMoreBatches);

    // Short circuit if record batch has already sent all data and is done
    if (state == BatchState.DONE) {
      return IterOutcome.NONE;
    }

    // keep saving incoming batches until the first unprocessed batch can be processed, or upstream == NONE
    while (!noMoreBatches && !framer.canDoWork()) {
      IterOutcome upstream = next(incoming);
      logger.trace("next(incoming) returned {}", upstream);

      switch (upstream) {
        case NONE:
          noMoreBatches = true;
          break;
        case OUT_OF_MEMORY:
        case NOT_YET:
        case STOP:
          cleanup();
          return upstream;
        case OK_NEW_SCHEMA:
          // We don't support schema changes
          if (!incoming.getSchema().equals(schema)) {
            if (schema != null) {
              throw new UnsupportedOperationException("OVER clause doesn't currently support changing schemas.");
            }
            this.schema = incoming.getSchema();
          }
        case OK:
          if (incoming.getRecordCount() > 0) {
            batches.add(new WindowDataBatch(incoming, oContext));
          } else {
            logger.trace("incoming has 0 records, it won't be added to batches");
          }
          break;
        default:
          throw new UnsupportedOperationException("Unsupported upstream state " + upstream);
      }
    }

    if (batches.isEmpty()) {
      logger.trace("no more batches to handle, we are DONE");
      state = BatchState.DONE;
      return IterOutcome.NONE;
    }

    // process first saved batch, then release it
    try {
      framer.doWork();
    } catch (DrillException e) {
      context.fail(e);
      cleanup();
      return IterOutcome.STOP;
    }

    if (state == BatchState.FIRST) {
      state = BatchState.NOT_FIRST;
    }

    return IterOutcome.OK;
  }

  @Override
  protected void buildSchema() throws SchemaChangeException {
    logger.trace("buildSchema()");
    IterOutcome outcome = next(incoming);
    switch (outcome) {
      case NONE:
        state = BatchState.DONE;
        container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
        return;
      case STOP:
        state = BatchState.STOP;
        return;
      case OUT_OF_MEMORY:
        state = BatchState.OUT_OF_MEMORY;
        return;
    }

    try {
      framer = createFramer(incoming);
    } catch (IOException | ClassTransformationException e) {
      throw new SchemaChangeException("Exception when creating the schema", e);
    }

    if (incoming.getRecordCount() > 0) {
      batches.add(new WindowDataBatch(incoming, oContext));
    }
  }

  private ValueVectorWriteExpression materializeLeadFunction(final LogicalExpression expression, final SchemaPath path,
                                                             final VectorAccessible batch, final ErrorCollector collector) {
    // read copied value from saved batch
    final LogicalExpression source = ExpressionTreeMaterializer.materialize(expression, batch,
      collector, context.getFunctionRegistry());

    // make sure window function vector type is Nullable, because we will write a null value in the last row
    // of each partition
    TypeProtos.MajorType majorType = source.getMajorType();
    if (majorType.getMode() == TypeProtos.DataMode.REQUIRED) {
      majorType = Types.optional(majorType.getMinorType());
    }

    // add corresponding ValueVector to container
    final MaterializedField outputField = MaterializedField.create(path, majorType);
    ValueVector vv = container.addOrGet(outputField);
    vv.allocateNew();

    // write copied value into container
    final TypedFieldId id =  container.getValueVectorId(vv.getField().getPath());
    return new ValueVectorWriteExpression(id, source, true);
  }

  private ValueVectorWriteExpression materializeLagFunction(final LogicalExpression expr, final SchemaPath path,
                                                            final VectorAccessible batch, final ErrorCollector collector) {
    // read copied value from saved batch
    final LogicalExpression source = ExpressionTreeMaterializer.materialize(expr, batch,
      collector, context.getFunctionRegistry());

    // make sure window function vector type is Nullable, because we will write a null value in the first row
    // of each partition
    TypeProtos.MajorType majorType = source.getMajorType();
    if (majorType.getMode() == TypeProtos.DataMode.REQUIRED) {
      majorType = Types.optional(majorType.getMinorType());
    }

    // add corresponding ValueVector to container
    final MaterializedField outputField = MaterializedField.create(path, majorType);
    ValueVector vv = container.addOrGet(outputField);
    vv.allocateNew();

    // write copied value into container
    final TypedFieldId id = container.getValueVectorId(vv.getField().getPath());
    return new ValueVectorWriteExpression(id, source, true);
  }

  private WindowFramer createFramer(VectorAccessible batch) throws SchemaChangeException, IOException, ClassTransformationException {
    assert framer == null : "createFramer should only be called once";

    final FunctionLookupContext registry = context.getFunctionRegistry();

    logger.trace("creating framer");

    final List<LogicalExpression> aggExprs = Lists.newArrayList();
    final List<WindowFunction> rankingFuncs = Lists.newArrayList();
    final List<LogicalExpression> keyExprs = Lists.newArrayList();
    final List<LogicalExpression> orderExprs = Lists.newArrayList();
    final List<LogicalExpression> leadExprs = Lists.newArrayList();
    final List<LogicalExpression> lagExprs = Lists.newArrayList();
    final List<LogicalExpression> internalExprs = Lists.newArrayList();
    final List<LogicalExpression> writeFirstValueToFirstValue = Lists.newArrayList();
    final List<LogicalExpression> writeSourceToFirstValue = Lists.newArrayList();
    final List<LogicalExpression> writeSourceToLastValue = Lists.newArrayList();
    final ErrorCollector collector = new ErrorCollectorImpl();

    container.clear();

    // all existing vectors will be transferred to the outgoing container in framer.doWork()
    for (VectorWrapper wrapper : batch) {
      container.addOrGet(wrapper.getField());
    }

    // add aggregation vectors to the container, and materialize corresponding expressions
    for (final NamedExpression ne : popConfig.getAggregations()) {
      final WindowFunction wf = WindowFunction.fromExpression(ne.getExpr());

      if (wf != null) {
        final FunctionCall call = (FunctionCall) ne.getExpr();

        switch (wf.type) {
          case LEAD:
            leadExprs.add(materializeLeadFunction(call.args.get(0), ne.getRef(), batch, collector));
          break;
          case LAG:
            {
              final ValueVectorWriteExpression writeExpr = materializeLagFunction(call.args.get(0), ne.getRef(), batch, collector);
              final TypedFieldId id = writeExpr.getFieldId();

              lagExprs.add(writeExpr);
              internalExprs.add(new ValueVectorWriteExpression(id, new ValueVectorReadExpression(id), true));
            }
            break;
          case FIRST_VALUE:
            {
              final LogicalExpression sourceRead = ExpressionTreeMaterializer.materialize(call.args.get(0), batch, collector,
                context.getFunctionRegistry());

              final MaterializedField firstValueColumn = MaterializedField.create(ne.getRef(), sourceRead.getMajorType());
              final ValueVector vv = container.addOrGet(firstValueColumn);
              vv.allocateNew();

              final TypedFieldId firstValueId = container.getValueVectorId(ne.getRef());

              // write incoming.first_value[inIndex] to outgoing.first_value[outIndex]
              writeFirstValueToFirstValue.add(new ValueVectorWriteExpression(firstValueId, new ValueVectorReadExpression(firstValueId), true));
              // write incoming.source[inIndex] to outgoing.first_value[outIndex]
              writeSourceToFirstValue.add(new ValueVectorWriteExpression(firstValueId, sourceRead, true));
            }
            break;
          case LAST_VALUE:
          {
            final LogicalExpression sourceRead = ExpressionTreeMaterializer.materialize(call.args.get(0), batch, collector,
              context.getFunctionRegistry());

            final MaterializedField lastValueColumn = MaterializedField.create(ne.getRef(), sourceRead.getMajorType());
            final ValueVector vv = container.addOrGet(lastValueColumn);
            vv.allocateNew();

            final TypedFieldId lastValueId = container.getValueVectorId(ne.getRef());

            // write incoming.source[inIndex] to outgoing.last_value[outIndex]
            writeSourceToLastValue.add(new ValueVectorWriteExpression(lastValueId, sourceRead, true));
          }
          break;
          case NTILE:
            ((WindowFunction.NtileWinFunc) wf).setNumTilesFromExpression(call.args.get(0));
            // fall-through
          default:
          {
            // add corresponding ValueVector to container
            final MaterializedField outputField = MaterializedField.create(ne.getRef(), wf.getMajorType());
            ValueVector vv = container.addOrGet(outputField);
            vv.allocateNew();

            wf.setFieldId(container.getValueVectorId(ne.getRef()));
            rankingFuncs.add(wf);
          }
          break;
        }
      } else {
        // evaluate expression over saved batch
        final LogicalExpression expr = ExpressionTreeMaterializer.materialize(ne.getExpr(), batch, collector, registry);

        // add corresponding ValueVector to container
        final MaterializedField outputField = MaterializedField.create(ne.getRef(), expr.getMajorType());
        ValueVector vv = container.addOrGet(outputField);
        vv.allocateNew();
        TypedFieldId id = container.getValueVectorId(ne.getRef());
        aggExprs.add(new ValueVectorWriteExpression(id, expr, true));
      }
    }

    if (container.isSchemaChanged()) {
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    }

    // materialize partition by expressions
    for (final NamedExpression ne : popConfig.getWithins()) {
      keyExprs.add(
        ExpressionTreeMaterializer.materialize(ne.getExpr(), batch, collector, context.getFunctionRegistry()));
    }

    // materialize order by expressions
    for (final Order.Ordering oe : popConfig.getOrderings()) {
      orderExprs.add(
        ExpressionTreeMaterializer.materialize(oe.getExpr(), batch, collector, context.getFunctionRegistry()));
    }

    if (collector.hasErrors()) {
      throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
    }

    final WindowFramer framer = generateFramer(keyExprs, orderExprs, aggExprs, rankingFuncs, leadExprs, lagExprs,
      internalExprs, writeFirstValueToFirstValue, writeSourceToFirstValue, writeSourceToLastValue);
    framer.setup(batches, container, oContext);

    return framer;
  }

  private WindowFramer generateFramer(final List<LogicalExpression> keyExprs, final List<LogicalExpression> orderExprs,
      final List<LogicalExpression> aggExprs, final List<WindowFunction> rankingFuncs,
      final List<LogicalExpression> leadExprs, final List<LogicalExpression> lagExprs,
      final List<LogicalExpression> internalExprs,
      final List<LogicalExpression> writeFirstValueToFirstValue, final List<LogicalExpression> writeSourceToFirstValue,
      final List<LogicalExpression> writeSourceToLastValue) throws IOException, ClassTransformationException {
    final ClassGenerator<WindowFramer> cg = CodeGenerator.getRoot(WindowFramer.TEMPLATE_DEFINITION, context.getFunctionRegistry());

    {
      // generating framer.isSamePartition()
      final GeneratorMapping IS_SAME_PARTITION_READ = GeneratorMapping.create("isSamePartition", "isSamePartition", null, null);
      final MappingSet isaB1 = new MappingSet("b1Index", null, "b1", null, IS_SAME_PARTITION_READ, IS_SAME_PARTITION_READ);
      final MappingSet isaB2 = new MappingSet("b2Index", null, "b2", null, IS_SAME_PARTITION_READ, IS_SAME_PARTITION_READ);
      setupIsFunction(cg, keyExprs, isaB1, isaB2);
    }

    {
      // generating framer.isPeer()
      final GeneratorMapping IS_SAME_PEER_READ = GeneratorMapping.create("isPeer", "isPeer", null, null);
      final MappingSet isaP1 = new MappingSet("b1Index", null, "b1", null, IS_SAME_PEER_READ, IS_SAME_PEER_READ);
      final MappingSet isaP2 = new MappingSet("b2Index", null, "b2", null, IS_SAME_PEER_READ, IS_SAME_PEER_READ);
      setupIsFunction(cg, orderExprs, isaP1, isaP2);
    }

    {
      // generating aggregations
      final GeneratorMapping EVAL_INSIDE = GeneratorMapping.create("setupEvaluatePeer", "evaluatePeer", null, null);
      final GeneratorMapping EVAL_OUTSIDE = GeneratorMapping.create("setupPartition", "outputRow", "resetValues", "cleanup");
      final MappingSet eval = new MappingSet("index", "outIndex", EVAL_INSIDE, EVAL_OUTSIDE, EVAL_INSIDE);
      generateForExpressions(cg, aggExprs, eval);
    }

    {
      // in DefaultFrameTemplate we call setupCopyFirstValue:
      //   setupCopyFirstValue(current, internal)
      // and copyFirstValueToInternal:
      //   copyFirstValueToInternal(currentRow, 0)
      //
      // this will generate the the following, pseudo, code:
      //   write current.source[currentRow] to internal.first_value[0]
      //
      // so it basically copies the first value of current partition into the first row of internal.first_value
      // this is especially useful when handling multiple batches for the same partition where we need to keep
      // the first value of the partition somewhere after we release the first batch

      final GeneratorMapping mapping = GeneratorMapping.create("setupCopyFirstValue", "copyFirstValueToInternal", null, null);
      final MappingSet mappingSet = new MappingSet("index", "0", mapping, mapping);
      generateForExpressions(cg, writeSourceToFirstValue, mappingSet);
    }

    {
      // in DefaultFrameTemplate we call setupPasteValues:
      //   setupPasteValues(internal, container)
      // and outputRow:
      //   outputRow(outIndex)
      //
      // this will generate the the following, pseudo, code:
      //   write internal.first_value[0] to container.first_value[outIndex]
      //
      // so it basically copies the value stored in internal.first_value's first row into all rows of container.first_value

      final GeneratorMapping mapping = GeneratorMapping.create("setupPasteValues", "outputRow", "resetValues", "cleanup");
      final MappingSet eval = new MappingSet("0", "outIndex", mapping, mapping);
      generateForExpressions(cg, writeFirstValueToFirstValue, eval);
    }

    {
      // in DefaultFrameTemplate we call setupReadLastValue:
      //   setupReadLastValue(current, container)
      // and readLastValue:
      //   writeLastValue(frameLastRow, row)
      //
      // this will generate the the following, pseudo, code:
      //   write current.source_last_value[frameLastRow] to container.last_value[row]

      final GeneratorMapping EVAL_INSIDE = GeneratorMapping.create("setupReadLastValue", "readLastValue", null, null);
      final GeneratorMapping EVAL_OUTSIDE = GeneratorMapping.create("setupReadLastValue", "writeLastValue", "resetValues", "cleanup");
      final MappingSet eval = new MappingSet("index", "outIndex", EVAL_INSIDE, EVAL_OUTSIDE, EVAL_INSIDE);
      generateForExpressions(cg, writeSourceToLastValue, eval);
    }

    {
      // generating lead copyNext
      final GeneratorMapping mapping = GeneratorMapping.create("setupCopyNext", "copyNext", null, null);
      final MappingSet eval = new MappingSet("inIndex", "outIndex", mapping, mapping);
      generateForExpressions(cg, leadExprs, eval);

    }

    {
      // generating lag copyPrev
      final GeneratorMapping mapping = GeneratorMapping.create("setupCopyPrev", "copyPrev", null, null);
      final MappingSet eval = new MappingSet("inIndex", "outIndex", mapping, mapping);
      generateForExpressions(cg, lagExprs, eval);
    }

    {
      // generating lag copyFromInternal
      final GeneratorMapping mapping = GeneratorMapping.create("setupCopyFromInternal", "copyFromInternal", null, null);
      final MappingSet eval = new MappingSet("inIndex", "outIndex", mapping, mapping);
      generateForExpressions(cg, internalExprs, eval);
    }

    generateRankingFunctions(cg, rankingFuncs);

    cg.getBlock("resetValues")._return(JExpr.TRUE);

    return context.getImplementationClass(cg);
  }

  /**
   * setup comparison functions isSamePartition and isPeer
   */
  private void setupIsFunction(final ClassGenerator<WindowFramer> cg, final List<LogicalExpression> exprs,
                               final MappingSet leftMapping, final MappingSet rightMapping) {
    cg.setMappingSet(leftMapping);
    for (LogicalExpression expr : exprs) {
      cg.setMappingSet(leftMapping);
      ClassGenerator.HoldingContainer first = cg.addExpr(expr, false);
      cg.setMappingSet(rightMapping);
      ClassGenerator.HoldingContainer second = cg.addExpr(expr, false);

      LogicalExpression fh =
        FunctionGenerationHelper
          .getOrderingComparatorNullsHigh(first, second, context.getFunctionRegistry());
      ClassGenerator.HoldingContainer out = cg.addExpr(fh, false);
      cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.getEvalBlock()._return(JExpr.TRUE);
  }

  /**
   * generate code to write "ranking" window function values into their respective value vectors
   */
  private void generateRankingFunctions(final ClassGenerator<WindowFramer> cg, final List<WindowFunction> functions) {
    final GeneratorMapping mapping = GeneratorMapping.create("setupPartition", "outputRow", "resetValues", "cleanup");
    final MappingSet eval = new MappingSet(null, "outIndex", mapping, mapping);

    cg.setMappingSet(eval);
    for (final WindowFunction wf : functions) {
      final JVar vv = cg.declareVectorValueSetupAndMember(cg.getMappingSet().getOutgoing(), wf.getFieldId());
      final JExpression outIndex = cg.getMappingSet().getValueWriteIndex();
      JInvocation setMethod;

      if (wf.type == WindowFunction.Type.NTILE) {
        final WindowFunction.NtileWinFunc ntilesFunc = (WindowFunction.NtileWinFunc)wf;
        setMethod = vv.invoke("getMutator").invoke("setSafe").arg(outIndex)
          .arg(JExpr.direct("partition.ntile(" + ntilesFunc.getNumTiles() + ")"));
      } else {
        setMethod = vv.invoke("getMutator").invoke("setSafe").arg(outIndex).arg(JExpr.direct("partition." + wf.getName()));
      }

      cg.getEvalBlock().add(setMethod);
    }
  }

  private void generateForExpressions(final ClassGenerator<WindowFramer> cg, final List<LogicalExpression> expressions,
                                      final MappingSet eval) {
    cg.setMappingSet(eval);
    for (LogicalExpression expr : expressions) {
      cg.addExpr(expr);
    }
  }

  private void cleanup() {
    if (framer != null) {
      framer.cleanup();
      framer = null;
    }

    if (batches != null) {
      for (final WindowDataBatch bd : batches) {
        bd.clear();
      }
      batches = null;
    }
  }

  @Override
  public void close() {
    cleanup();
    super.close();
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

  @Override
  public int getRecordCount() {
    return framer.getOutputCount();
  }
}
