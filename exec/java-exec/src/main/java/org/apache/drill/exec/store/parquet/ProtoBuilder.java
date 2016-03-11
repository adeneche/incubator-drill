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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import org.apache.drill.exec.store.parquet.proto.Metadata.MetadataHeader;
import org.apache.drill.exec.store.parquet.proto.Metadata.MetadataColumns;
import org.apache.drill.exec.store.parquet.proto.Metadata.MetadataColumns.ColumnTypeInfo;
import org.apache.drill.exec.store.parquet.proto.Metadata.ParquetFileMetadata;
import org.apache.drill.exec.store.parquet.proto.Metadata.ParquetFileMetadata.RowGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class ProtoBuilder {
  public static final Joiner COLUMN_NAME_JOINER = Joiner.on(".");

  private final Metadata.ParquetTableMetadata_v2 tableMetadata;

  private final List<String> columnNames = Lists.newArrayList();

  public ProtoBuilder(final Metadata.ParquetTableMetadata_v2 tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  public MetadataHeader buildHeader(final String version) {
    return MetadataHeader.newBuilder()
      .setMetadataVersion(version)
      .addAllDirectories(tableMetadata.directories)
      .build();
  }

  public MetadataColumns buildColumns() {
    final MetadataColumns.Builder metadataColumns = MetadataColumns.newBuilder();

    List<ColumnTypeInfo> columns = Lists.newArrayList();
    for (final Metadata.ColumnTypeMetadata_v2.Key key : tableMetadata.columnTypeInfo.keySet()) {
      final ColumnTypeInfo columnTypeInfo = buildColumnTypeInfo(tableMetadata.columnTypeInfo.get(key));
      columns.add(columnTypeInfo);
    }

    //make sure we sort keySet so we always have same order
    Collections.sort(columns, new Comparator<ColumnTypeInfo>() {
      @Override
      public int compare(ColumnTypeInfo o1, ColumnTypeInfo o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    metadataColumns.addAllColumns(columns);

    Iterables.addAll(columnNames, Iterables.transform(columns, new Function<ColumnTypeInfo, String>() {
      @Override
      public String apply(ColumnTypeInfo input) {
        return input.getName();
      }
    }));

    return metadataColumns.build();
  }

  public List<ParquetFileMetadata> buildFiles() {
    List<ParquetFileMetadata> files = Lists.newArrayList();

    for (final Metadata.ParquetFileMetadata_v2 fileMetadata : tableMetadata.files) {
      files.add(buildFile(fileMetadata));
    }

    return files;
  }

  static ColumnTypeInfo.PrimitiveTypeName convert(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
    return ColumnTypeInfo.PrimitiveTypeName.valueOf(primitiveTypeName.name());
  }

  static PrimitiveType.PrimitiveTypeName convert(ColumnTypeInfo.PrimitiveTypeName primitiveTypeName) {
    return PrimitiveType.PrimitiveTypeName.valueOf(primitiveTypeName.name());
  }

  static OriginalType convert(ColumnTypeInfo.OriginalType originalType) {
    return OriginalType.valueOf(originalType.name());
  }

  static ColumnTypeInfo.OriginalType convert(OriginalType originalType) {
    return ColumnTypeInfo.OriginalType.valueOf(originalType.name());
  }

  private ColumnTypeInfo buildColumnTypeInfo(Metadata.ColumnTypeMetadata_v2 columnTypeMetadata) {
    final ColumnTypeInfo.Builder columnTypeInfo = ColumnTypeInfo.newBuilder();

    columnTypeInfo.setName(COLUMN_NAME_JOINER.join(columnTypeMetadata.name));

    if (columnTypeMetadata.primitiveType != null) {
      columnTypeInfo.setPrimitiveType(convert(columnTypeMetadata.primitiveType));
    }

    if (columnTypeMetadata.originalType != null) {
      columnTypeInfo.setOriginalType(convert(columnTypeMetadata.originalType));
    }

    return columnTypeInfo.build();
  }

  private ParquetFileMetadata buildFile(Metadata.ParquetFileMetadata_v2 fileMetadata) {
    final ParquetFileMetadata.Builder file = ParquetFileMetadata.newBuilder();

    file.setPath(fileMetadata.path);
    file.setLength(fileMetadata.length);

    for (final Metadata.RowGroupMetadata_v2 rowGroupMetadata : fileMetadata.rowGroups) {
      file.addRowGroups(buildRowGroup(rowGroupMetadata));
    }

    return file.build();
  }

  private RowGroup buildRowGroup(Metadata.RowGroupMetadata_v2 rowGroupMetadata) {
    final RowGroup.Builder rowGroup = RowGroup.newBuilder();

    rowGroup.setStart(rowGroupMetadata.start);
    rowGroup.setLength(rowGroupMetadata.length);
    rowGroup.setRowCount(rowGroupMetadata.rowCount);

    final Map<String, Float> hostAffinity = rowGroupMetadata.hostAffinity;
    for (final String name : hostAffinity.keySet()) {
      rowGroup.addAffinities(RowGroup.HostAffinity.newBuilder()
          .setKey(name)
          .setValue(hostAffinity.get(name))
      );
    }

    for (final Metadata.ColumnMetadata_v2 column : rowGroupMetadata.columns) {
      final RowGroup.ColumnMetadata columnMetadata = buildRowGroupColumn(column);
      if (columnMetadata != null) {
        rowGroup.addColumns(columnMetadata);
      }
    }

    return rowGroup.build();
  }

  private RowGroup.ColumnMetadata buildRowGroupColumn(Metadata.ColumnMetadata_v2 column) {
    Long nulls = column.nulls;
    if (nulls != null && nulls == 0) {
      nulls = null;
    }
    final Object mxValue = column.mxValue;

    if (nulls == null && mxValue == null) {
      return null;
    }

    final RowGroup.ColumnMetadata.Builder columnMetadata = RowGroup.ColumnMetadata.newBuilder();

    final int nameId = getColumnId(column.name);
    columnMetadata.setName(nameId);

    if (nulls != null) {
      columnMetadata.setNulls(nulls);
    }

    if (mxValue != null) {
      if (mxValue instanceof Long) {
        columnMetadata.setVint64((Long) mxValue);
      } else if (mxValue instanceof Integer) {
        columnMetadata.setVint32((Integer) mxValue);
      } else if (mxValue instanceof Boolean) {
        columnMetadata.setVbool((Boolean) mxValue);
      } else if (mxValue instanceof Binary) {
        columnMetadata.setVbinary(ByteString.copyFrom(((Binary) mxValue).getBytes()));
      } else if (mxValue instanceof Float) {
        columnMetadata.setVfloat((Float) mxValue);
      } else if (mxValue instanceof Double) {
        columnMetadata.setVdouble((Double) mxValue);
      } else {
        throw new RuntimeException("Unrecognized mxValue type: " + mxValue.getClass().getSimpleName());
      }
    }

    return columnMetadata.build();
  }

  private int getColumnId(final String[] columnName) {
    final String name = COLUMN_NAME_JOINER.join(columnName);
    final int nameId = columnNames.indexOf(name);
    if (nameId == -1) {
      throw new RuntimeException("column '" + name + "' not found in columnNames");
    }
    return nameId;
  }

}
