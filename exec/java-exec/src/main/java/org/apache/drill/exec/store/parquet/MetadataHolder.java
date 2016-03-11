/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.drill.exec.store.parquet.proto.Metadata.MetadataHeader;
import org.apache.drill.exec.store.parquet.proto.Metadata.MetadataColumns;
import org.apache.drill.exec.store.parquet.proto.Metadata.MetadataColumns.ColumnTypeInfo;
import org.apache.drill.exec.store.parquet.proto.Metadata.ParquetFileMetadata;
import org.apache.drill.exec.store.parquet.proto.Metadata.ParquetFileMetadata.RowGroup;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

class MetadataHolder {
  private MetadataHeader header;
  private MetadataColumns columns;
  private List<ParquetFileMetadata> files;

  final static Splitter SPLITTER = Splitter.on('.');

  public void parseFrom(CodedInputStream codedStream) throws IOException {
    // read header
    int length = codedStream.readRawVarint32();
    int limit = codedStream.pushLimit(length);
    header = MetadataHeader.parseFrom(codedStream);
    codedStream.popLimit(limit);
    codedStream.resetSizeCounter();

    // read columns
    length = codedStream.readRawVarint32();
    limit = codedStream.pushLimit(length);
    columns = MetadataColumns.parseFrom(codedStream);
    codedStream.popLimit(limit);
    codedStream.resetSizeCounter();

    // read files
    int numFiles = codedStream.readRawVarint32();
    files = Lists.newArrayList();
    for (int i = 0; i < numFiles; i++) {
      length = codedStream.readRawVarint32();
      limit = codedStream.pushLimit(length);
      files.add(ParquetFileMetadata.parseFrom(codedStream));
      codedStream.popLimit(limit);
      codedStream.resetSizeCounter();
    }
  }

  public void writeTo(CodedOutputStream codedStream) throws IOException {
    codedStream.writeRawVarint32(header.getSerializedSize());
    header.writeTo(codedStream);

    codedStream.writeRawVarint32(columns.getSerializedSize());
    columns.writeTo(codedStream);

    codedStream.writeRawVarint32(files.size());
    for (final ParquetFileMetadata file : files) {
      codedStream.writeRawVarint32(file.getSerializedSize());
      file.writeTo(codedStream);
    }

    codedStream.flush();
  }

  public void parseFrom(String path) throws IOException {
    CodedInputStream codedStream = CodedInputStream.newInstance(new FileInputStream(path));
    codedStream.setSizeLimit(200_000_000); //TODO we should no longer need this

    parseFrom(codedStream);
  }

  public void parseFrom(Metadata.ParquetTableMetadata_v2 tableMetadata) {
    final ProtoBuilder protoBuilder = new ProtoBuilder(tableMetadata);

    header = protoBuilder.buildHeader("v2");
    columns = protoBuilder.buildColumns();
    files = protoBuilder.buildFiles();
  }

  public Metadata.ParquetTableMetadata_v2 toParquetTableMetadata() {
    final Metadata.ParquetTableMetadata_v2 tableMetadata = new Metadata.ParquetTableMetadata_v2();

    tableMetadata.columnTypeInfo = Maps.newHashMap();
    for (final ColumnTypeInfo column : columns.getColumnsList()) {
      final Metadata.ColumnTypeMetadata_v2 columnMetadata = new Metadata.ColumnTypeMetadata_v2(
        Iterables.toArray(SPLITTER.split(column.getName()), String.class),
        ProtoBuilder.convert(column.getPrimitiveType()),
        ProtoBuilder.convert(column.getOriginalType())
      );
      tableMetadata.columnTypeInfo.put(columnMetadata.key(), columnMetadata);
    }

    tableMetadata.files = Lists.newArrayList();
    for (final ParquetFileMetadata file : files) {
      final Metadata.ParquetFileMetadata_v2 fileMetadata = new Metadata.ParquetFileMetadata_v2();
      fileMetadata.path = file.getPath();
      fileMetadata.length = file.getLength();
      fileMetadata.rowGroups = Lists.newArrayList();
      for (final RowGroup rowGroup : file.getRowGroupsList()) {
        final Metadata.RowGroupMetadata_v2 rowGroupMetadata = new Metadata.RowGroupMetadata_v2();
        rowGroupMetadata.start = rowGroup.getStart();
        rowGroupMetadata.length = rowGroup.getLength();
        rowGroupMetadata.rowCount = rowGroup.getRowCount();

        rowGroupMetadata.hostAffinity = Maps.newHashMap();
        for (final RowGroup.HostAffinity affinity : rowGroup.getAffinitiesList()) {
          rowGroupMetadata.hostAffinity.put(affinity.getKey(), affinity.getValue());
        }

        rowGroupMetadata.columns = Lists.newArrayList();
        for (final RowGroup.ColumnMetadata rowGroupColumn : rowGroup.getColumnsList()) {
          final MetadataColumns.ColumnTypeInfo colMeta = columns.getColumns(rowGroupColumn.getName());
          final Metadata.ColumnMetadata_v2 columnMetadata = new Metadata.ColumnMetadata_v2(
            Iterables.toArray(SPLITTER.split(colMeta.getName()), String.class),
            ProtoBuilder.convert(colMeta.getPrimitiveType()),
            getMxValue(rowGroupColumn),
            rowGroupColumn.getNulls()
          );
          rowGroupMetadata.columns.add(columnMetadata);
        }
        fileMetadata.rowGroups.add(rowGroupMetadata);
      }
      tableMetadata.files.add(fileMetadata);
    }

    tableMetadata.directories = header.getDirectoriesList();

    return tableMetadata;
  }

  private static Object getMxValue(final RowGroup.ColumnMetadata columnMetadata) {
    if (columnMetadata.hasVbinary()) {
      return columnMetadata.getVbinary();
    } else if (columnMetadata.hasVbool()) {
      return columnMetadata.getVbool();
    } else if (columnMetadata.hasVdouble()) {
      return columnMetadata.getVdouble();
    } else if (columnMetadata.hasVfloat()) {
      return columnMetadata.getVfloat();
    } else if (columnMetadata.hasVint32()) {
      return columnMetadata.getVint32();
    } else if (columnMetadata.hasVint64()) {
      return columnMetadata.getVint64();
    }

    return null;
  }

  @Override
  public String toString() {
    return "" + header + "\n" + columns + "\n" + files;
  }
}
