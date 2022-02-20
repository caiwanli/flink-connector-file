package org.apache.flink.connector.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.enumerator.FileSourceEnumerator;
import org.apache.flink.connector.enumerator.PendingSplitsCheckpoint;
import org.apache.flink.connector.enumerator.PendingSplitsCheckpointSerializer;
import org.apache.flink.connector.reader.FileSourceReader;
import org.apache.flink.connector.split.FileSourceSplit;
import org.apache.flink.connector.split.FileSourceSplitSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** */
public class FileSource implements Source<RowData, FileSourceSplit, PendingSplitsCheckpoint>, ResultTypeQueryable<RowData> {
    private final TypeInformation<RowData> producedTypeInfo;
    private final Path[] paths;

    public FileSource(TypeInformation<RowData> producedTypeInfo, Path[] paths) {
        this.producedTypeInfo = checkNotNull(producedTypeInfo);
        this.paths = checkNotNull(paths);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<RowData, FileSourceSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
        return new FileSourceReader(sourceReaderContext);
    }

    @Override
    public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> createEnumerator(SplitEnumeratorContext<FileSourceSplit> splitEnumeratorContext) throws Exception {
        return new FileSourceEnumerator(
                splitEnumeratorContext, paths, Collections.emptyList(), Collections.emptyList());
    }

    @Override
    public SplitEnumerator<FileSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(SplitEnumeratorContext<FileSourceSplit> splitEnumeratorContext, PendingSplitsCheckpoint pendingSplitsCheckpoint) throws Exception {
        return new FileSourceEnumerator(
                splitEnumeratorContext, paths, pendingSplitsCheckpoint.getSplits(), pendingSplitsCheckpoint.getAlreadyProcessedPaths());
    }

    @Override
    public SimpleVersionedSerializer<FileSourceSplit> getSplitSerializer() {
        return FileSourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsCheckpoint> getEnumeratorCheckpointSerializer() {
        return new PendingSplitsCheckpointSerializer(getSplitSerializer());
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }
}