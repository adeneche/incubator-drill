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

package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;

import javax.inject.Inject;

public class Fizz {

    /**
     * Returns the char corresponding to ASCII code input.
     */
    @SuppressWarnings("UnusedDeclaration")
    @FunctionTemplate(name = "fizz", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
    public static class AsciiToChar implements DrillSimpleFunc {

        @Param
        IntHolder in;
        @Output
        VarCharHolder out;
        @Inject
        DrillBuf buf;

        public void setup(RecordBatch incoming) {
            buf = buf.reallocIfNeeded(1);
        }

        public void eval() {
            out.buffer = buf;
            out.start = out.end = 0;
            out.buffer.setByte(0, in.value);
            ++out.end;
        }
    }
}