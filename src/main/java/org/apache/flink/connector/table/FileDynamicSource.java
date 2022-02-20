package org.apache.flink.connector.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.source.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class FileDynamicSource implements ScanTableSource {

    private final Path[] paths;
    final DataType producedDataType;

    public FileDynamicSource(String path, DataType producedDataType) {
        this(Stream.of(checkNotNull(path)).map(Path::new).toArray(Path[]::new), producedDataType);
    }

    public FileDynamicSource(Path[] paths, DataType producedDataType) {
        this.paths = checkNotNull(paths);
        this.producedDataType = checkNotNull(producedDataType);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        final TypeInformation<RowData> producedTypeInfo =
                scanContext.createTypeInformation(this.producedDataType);

        return SourceProvider.of(new FileSource(producedTypeInfo, paths));
    }

    @Override
    public DynamicTableSource copy() {
        return new FileDynamicSource(paths, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "File Connector Source";
    }

    //-----------------------------------------help-----------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileDynamicSource that = (FileDynamicSource) o;
        return Arrays.equals(paths, that.paths) && Objects.equals(producedDataType, that.producedDataType);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(producedDataType);
        result = 31 * result + Arrays.hashCode(paths);
        return result;
    }

    @Override
    public String toString() {
        return "FileDynamicSource{"
                + "paths="
                + Arrays.toString(paths)
                + ", producedDataType="
                + producedDataType
                + '}';
    }
}
