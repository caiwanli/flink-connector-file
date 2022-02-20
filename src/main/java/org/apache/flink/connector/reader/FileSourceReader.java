package org.apache.flink.connector.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.split.FileSourceSplit;
import org.apache.flink.table.data.RowData;
import java.util.Map;

public class FileSourceReader extends SingleThreadMultiplexSourceReaderBase<RecordAndPosition, RowData, FileSourceSplit, FileSourceSplitState> {

    public FileSourceReader(SourceReaderContext readerContext) {
        super(
                FileSourceSplitReader::new,
                new FileSourceRecordEmitter(),
                readerContext.getConfiguration(),
                readerContext);
    }

    @Override
    public void start() {
        // we request a split only if we did not get splits during the checkpoint restore
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, FileSourceSplitState> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    protected FileSourceSplitState initializedState(FileSourceSplit fileSourceSplit) {
        return new FileSourceSplitState(fileSourceSplit);
    }

    @Override
    protected FileSourceSplit toSplitType(String splitId, FileSourceSplitState fileSourceSplitState) {
        return fileSourceSplitState.toFileSourceSplit();
    }
}
