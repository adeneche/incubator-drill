/*******************************************************************************

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
 ******************************************************************************/


package com.tdunning.drill.exec.expr.fn.impl;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import javax.inject.Inject;

/*
 * Look up values in an HBase or MapR DB table.
 */

@SuppressWarnings("unused")
public class HbaseLookup {
    public static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HbaseLookup.class);

    private HbaseLookup(){}

    @FunctionTemplate(name = "hLookup", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
    public static class Lookup implements DrillSimpleFunc {

        @Param VarCharHolder table;                // the table to read from
        @Param VarCharHolder columns;              // which columns to retrieve, comma-delimited
        @Param NullableVarCharHolder key;          // the key to read
        @Output BaseWriter.ComplexWriter writer;

        // this class just simplifies writing code by being outside the reach of the code generator
        @Workspace com.tdunning.drill.exec.expr.fn.impl.LookupHelper helper;

        @Inject DrillBuf buffer; // buffer used internally

        public void setup(RecordBatch b) {
            helper = new com.tdunning.drill.exec.expr.fn.impl.LookupHelper(buffer);
        }

        public void eval() {
            if (key.isSet == 0) {
                return;
            }

            java.nio.charset.Charset utf8 = com.google.common.base.Charsets.UTF_8;
            String tableStr = table.buffer.toString(table.start, table.end - table.start, utf8);
            String columnsStr = columns.buffer.toString(columns.start, columns.end - columns.start, utf8);

            byte[] bytes = new byte[key.end - key.start];
            key.buffer.getBytes(key.start, bytes, 0, key.end - key.start);

            // when generating code, Drill changes the types of the input variables,
            // then complains LookupHelper.lookup() doesn't match the signature
            // I am probably missing something, but it's just easier to pass the values
            helper.lookup(tableStr, columnsStr, bytes, writer);
        }
    }
}