package org.apache.flink.connector.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.table.data.RowData;

public class FileSourceRecordEmitter implements RecordEmitter<RecordAndPosition, RowData, FileSourceSplitState> {
    @Override
    public void emitRecord(RecordAndPosition recordAndPosition, SourceOutput<RowData> sourceOutput, FileSourceSplitState fileSourceSplitState) throws Exception {
        sourceOutput.collect(recordAndPosition.getRecord());
        fileSourceSplitState.setOffset(recordAndPosition.getOffset());
    }
}
