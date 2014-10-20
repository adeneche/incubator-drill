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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Look up keys in a table. We presume that the table name and column names will rarely change so we cache the
 * actual HBase table and the split apart column names.
 * <p/>
 * Column names can be specified as columnFamily:columnName to specify a single column or columnFamily:* to
 * specify all columns in a particular family.
 */
public class LookupHelper {
    private final ThreadLocal<String> tableName = new ThreadLocal<>();
    private final ThreadLocal<HTable> tbl = new ThreadLocal<>();

    private final ThreadLocal<String> columns = new ThreadLocal<>();
    private final ThreadLocal<List<ColumSpec>> specs = new ThreadLocal<>();

    public void lookup(NullableVarCharHolder table, NullableVarCharHolder columns, NullableVarCharHolder key, BaseWriter.ComplexWriter writer) {
        Preconditions.checkArgument(table.isSet > 0, "Must have valid table, got null instead");
        Preconditions.checkArgument(columns.isSet > 0, "Must have valid column spec, got null instead");
        if (key.isSet == 0) {
            // null key, null result
            return;
        }

        // cache the table
        String name = table.buffer.toString(Charsets.UTF_8);
        if (tbl.get() == null || !name.equals(tableName.get())) {
            tableName.set(name);
            try {
                tbl.set(new HTable(new Configuration(), tableName.get()));
            } catch (IOException e) {
                throw new IllegalArgumentException(String.format("Can't open table %s", name), e);
            }
        }

        // cache the column specifications
        String columnNames = table.buffer.toString(Charsets.UTF_8);
        if (!columnNames.equals(this.columns.get())) {
            specs.set(null);
            Splitter onComma = Splitter.on(",").trimResults().omitEmptyStrings();
            Pattern columnPattern = Pattern.compile("(\\w+):((\\w*)|\\*)");
            List<ColumSpec> columnSpec = Lists.newArrayList();
            for (String column : onComma.split(columnNames)) {
                Matcher m = columnPattern.matcher(column);
                Preconditions.checkState(m.matches(), String.format("Invalid column specification %s", column));
                columnSpec.add(new ColumSpec(m.group(1), m.group(2)));
            }
            specs.set(columnSpec);
        }

        // set up the table read
        byte[] rowKey = new byte[key.end - key.start];
        key.buffer.getBytes(key.start, rowKey, 0, key.end - key.start);
        Get g = new Get(rowKey);
        for (ColumSpec spec : specs.get()) {
            spec.add(g);
        }

        // read from HBase
        try {
            Result r = tbl.get().get(g);

            BaseWriter.MapWriter mw = writer.rootAsMap();

            mw.start();

            NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultMap = r.getNoVersionMap();
            for (byte[] family : resultMap.keySet()) {
                String familyName = new String(family, Charsets.UTF_8);

                NavigableMap<byte[], byte[]> familyMap = resultMap.get(family);
                for (byte[] column : familyMap.keySet()) {
                    VarBinaryHolder binaryHolder = new VarBinaryHolder();
                    binaryHolder.buffer.setBytes(0, familyMap.get(column));
                    String resultColumn = familyName + ":" + new String(column, Charsets.UTF_8);
                    mw.varBinary(resultColumn).write(binaryHolder);
                }
            }

            mw.end();
        } catch (IOException e) {
            // returns null
        }
    }

    private static class ColumSpec {
        private final byte[] family;
        private final byte[] column;

        public ColumSpec(String family, String column) {
            this.family = family.getBytes(Charsets.UTF_8);
            if (!"*".equals(column)) {
                this.column = column.getBytes(Charsets.UTF_8);
            } else {
                this.column = null;
            }
        }

        public void add(Get g) {
            if (column == null) {
                g.addFamily(family);
            } else {
                g.addColumn(family, column);
            }
        }
    }
}
