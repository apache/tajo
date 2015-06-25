/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tajo.storage.thirdparty.orc;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import org.apache.tajo.storage.thirdparty.orc.checkpoint.StreamCheckpoint;
import org.apache.tajo.storage.thirdparty.orc.metadata.*;
import org.apache.tajo.storage.thirdparty.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import org.apache.tajo.storage.thirdparty.orc.metadata.OrcType.OrcTypeKind;
import org.apache.tajo.storage.thirdparty.orc.stream.*;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.tajo.storage.thirdparty.orc.checkpoint.Checkpoints.getDictionaryStreamCheckpoint;
import static org.apache.tajo.storage.thirdparty.orc.checkpoint.Checkpoints.getStreamCheckpoints;
import static org.apache.tajo.storage.thirdparty.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static org.apache.tajo.storage.thirdparty.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static org.apache.tajo.storage.thirdparty.orc.metadata.Stream.StreamKind.*;
import static org.apache.tajo.storage.thirdparty.orc.stream.CheckpointStreamSource.createCheckpointStreamSource;

public class StripeReader
{
    private final OrcDataSource orcDataSource;
    private final CompressionKind compressionKind;
    private final List<OrcType> types;
    private final int bufferSize;
    private final Set<Integer> includedOrcColumns;
    private final int rowsInRowGroup;
    private final OrcPredicate predicate;
    private final MetadataReader metadataReader;

    public StripeReader(OrcDataSource orcDataSource,
            CompressionKind compressionKind,
            List<OrcType> types,
            int bufferSize,
            Set<Integer> includedColumns,
            int rowsInRowGroup,
            OrcPredicate predicate,
            MetadataReader metadataReader)
    {
        this.orcDataSource = checkNotNull(orcDataSource, "orcDataSource is null");
        this.compressionKind = checkNotNull(compressionKind, "compressionKind is null");
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
        this.bufferSize = bufferSize;
        this.includedOrcColumns = getIncludedOrcColumns(types, checkNotNull(includedColumns, "includedColumns is null"));
        this.rowsInRowGroup = rowsInRowGroup;
        this.predicate = checkNotNull(predicate, "predicate is null");
        this.metadataReader = checkNotNull(metadataReader, "metadataReader is null");
    }

    public Stripe readStripe(StripeInformation stripe)
            throws IOException
    {
        // read the stripe footer
        StripeFooter stripeFooter = readStripeFooter(stripe);
        List<ColumnEncoding> columnEncodings = stripeFooter.getColumnEncodings();

        // get streams for selected columns
        Map<StreamId, Stream> streams = new HashMap<StreamId, Stream>();
        for (Stream stream : stripeFooter.getStreams()) {
            if (includedOrcColumns.contains(stream.getColumn())) {
                streams.put(new StreamId(stream), stream);
            }
        }

        // determine ranges of the stripe to read
        Map<StreamId, DiskRange> diskRanges = getDiskRanges(stripeFooter.getStreams());
        diskRanges = Maps.filterKeys(diskRanges, Predicates.in(streams.keySet()));

        // read the file regions
        Map<StreamId, OrcInputStream> streamsData = readDiskRanges(stripe.getOffset(), diskRanges);

        // read the row index for each column
        Map<Integer, List<RowGroupIndex>> columnIndexes = readColumnIndexes(streams, streamsData);

        // select the row groups matching the tuple domain
        Set<Integer> selectedRowGroups = selectRowGroups(stripe, columnIndexes);

        // if all row groups are skipped, return null
        if (selectedRowGroups.isEmpty()) {
            return null;
        }

        // value streams
        Map<StreamId, ValueStream<?>> valueStreams = createValueStreams(streams, streamsData, columnEncodings);

        // build the dictionary streams
        StreamSources dictionaryStreamSources = createDictionaryStreamSources(streams, valueStreams, columnEncodings);

        // build the row groups
        List<RowGroup> rowGroups = createRowGroups(
                stripe.getNumberOfRows(),
                streams,
                valueStreams,
                columnIndexes,
                selectedRowGroups,
                columnEncodings);

        return new Stripe(stripe.getNumberOfRows(), columnEncodings, rowGroups, dictionaryStreamSources);
    }

    public Map<StreamId, OrcInputStream> readDiskRanges(final long stripeOffset, Map<StreamId, DiskRange> diskRanges)
            throws IOException
    {
        // transform ranges to have an absolute offset in file
        diskRanges = Maps.transformValues(diskRanges, new Function<DiskRange, DiskRange>() {
            @Override
            public DiskRange apply(DiskRange diskRange)
            {
                return new DiskRange(stripeOffset + diskRange.getOffset(), diskRange.getLength());
            }
        });

        Map<StreamId, Slice> streamsData = orcDataSource.readFully(diskRanges);

        return ImmutableMap.copyOf(Maps.transformValues(streamsData, new Function<Slice, OrcInputStream>()
        {
            @Override
            public OrcInputStream apply(Slice input)
            {
                return new OrcInputStream(orcDataSource.toString(), input.getInput(), compressionKind, bufferSize);
            }
        }));
    }

    private Map<StreamId, ValueStream<?>> createValueStreams(Map<StreamId, Stream> streams, Map<StreamId, OrcInputStream> streamsData, List<ColumnEncoding> columnEncodings)
    {
        ImmutableMap.Builder<StreamId, ValueStream<?>> valueStreams = ImmutableMap.builder();
        for (Entry<StreamId, Stream> entry : streams.entrySet()) {
            StreamId streamId = entry.getKey();
            Stream stream = entry.getValue();
            ColumnEncodingKind columnEncoding = columnEncodings.get(stream.getColumn()).getColumnEncodingKind();

            // skip index and empty streams
            if (isIndexStream(stream) || stream.getLength() == 0) {
                continue;
            }

            OrcInputStream inputStream = streamsData.get(streamId);
            OrcTypeKind columnType = types.get(stream.getColumn()).getOrcTypeKind();

            valueStreams.put(streamId, ValueStreams.createValueStreams(streamId, inputStream, columnType, columnEncoding, stream.isUseVInts()));
        }
        return valueStreams.build();
    }

    public StreamSources createDictionaryStreamSources(Map<StreamId, Stream> streams, Map<StreamId, ValueStream<?>> valueStreams, List<ColumnEncoding> columnEncodings)
    {
        ImmutableMap.Builder<StreamId, StreamSource<?>> dictionaryStreamBuilder = ImmutableMap.builder();
        for (Entry<StreamId, Stream> entry : streams.entrySet()) {
            StreamId streamId = entry.getKey();
            Stream stream = entry.getValue();
            int column = stream.getColumn();

            // only process dictionary streams
            ColumnEncodingKind columnEncoding = columnEncodings.get(column).getColumnEncodingKind();
            if (!isDictionary(stream, columnEncoding)) {
                continue;
            }

            // skip streams without data
            ValueStream<?> valueStream = valueStreams.get(streamId);
            if (valueStream == null) {
                continue;
            }

            OrcTypeKind columnType = types.get(stream.getColumn()).getOrcTypeKind();
            StreamCheckpoint streamCheckpoint = getDictionaryStreamCheckpoint(streamId, columnType, columnEncoding);

            StreamSource<?> streamSource = createCheckpointStreamSource(valueStream, streamCheckpoint);
            dictionaryStreamBuilder.put(streamId, streamSource);
        }
        return new StreamSources(dictionaryStreamBuilder.build());
    }

    private List<RowGroup> createRowGroups(
            int rowsInStripe,
            Map<StreamId, Stream> streams,
            Map<StreamId, ValueStream<?>> valueStreams,
            Map<Integer, List<RowGroupIndex>> columnIndexes,
            Set<Integer> selectedRowGroups,
            List<ColumnEncoding> encodings)
    {
        ImmutableList.Builder<RowGroup> rowGroupBuilder = ImmutableList.builder();

        for (int rowGroupId : selectedRowGroups) {
            Map<StreamId, StreamCheckpoint> checkpoints = getStreamCheckpoints(includedOrcColumns, types, compressionKind, rowGroupId, encodings, streams, columnIndexes);
            int rowsInGroup = Math.min(rowsInStripe - (rowGroupId * rowsInRowGroup), rowsInRowGroup);
            rowGroupBuilder.add(createRowGroup(rowGroupId, rowsInGroup, valueStreams, checkpoints));
        }

        return rowGroupBuilder.build();
    }

    public static RowGroup createRowGroup(int groupId, int rowCount, Map<StreamId, ValueStream<?>> valueStreams, Map<StreamId, StreamCheckpoint> checkpoints)
    {
        ImmutableMap.Builder<StreamId, StreamSource<?>> builder = ImmutableMap.builder();
        for (Entry<StreamId, StreamCheckpoint> entry : checkpoints.entrySet()) {
            StreamId streamId = entry.getKey();
            StreamCheckpoint checkpoint = entry.getValue();

            // skip streams without data
            ValueStream<?> valueStream = valueStreams.get(streamId);
            if (valueStream == null) {
                continue;
            }

            builder.put(streamId, createCheckpointStreamSource(valueStream, checkpoint));
        }
        StreamSources rowGroupStreams = new StreamSources(builder.build());
        return new RowGroup(groupId, rowCount, rowGroupStreams);
    }

    public StripeFooter readStripeFooter(StripeInformation stripe)
            throws IOException
    {
        long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
        int tailLength = Ints.checkedCast(stripe.getFooterLength());

        // read the footer
        byte[] tailBuffer = new byte[tailLength];
        orcDataSource.readFully(offset, tailBuffer);
        InputStream inputStream = new OrcInputStream(orcDataSource.toString(), Slices.wrappedBuffer(tailBuffer).getInput(), compressionKind, bufferSize);
        return metadataReader.readStripeFooter(types, inputStream);
    }

    private Map<Integer, List<RowGroupIndex>> readColumnIndexes(Map<StreamId, Stream> streams, Map<StreamId, OrcInputStream> streamsData)
            throws IOException
    {
        ImmutableMap.Builder<Integer, List<RowGroupIndex>> columnIndexes = ImmutableMap.builder();
        for (Entry<StreamId, Stream> entry : streams.entrySet()) {
            Stream stream = entry.getValue();
            if (stream.getStreamKind() == ROW_INDEX) {
                OrcInputStream inputStream = streamsData.get(entry.getKey());
                columnIndexes.put(stream.getColumn(), metadataReader.readRowIndexes(inputStream));
            }
        }
        return columnIndexes.build();
    }

    private Set<Integer> selectRowGroups(StripeInformation stripe,  Map<Integer, List<RowGroupIndex>> columnIndexes)
            throws IOException
    {
        int rowsInStripe = Ints.checkedCast(stripe.getNumberOfRows());
        int groupsInStripe = ceil(rowsInStripe, rowsInRowGroup);

        ImmutableSet.Builder<Integer> selectedRowGroups = ImmutableSet.builder();
        int remainingRows = rowsInStripe;
        for (int rowGroup = 0; rowGroup < groupsInStripe; ++rowGroup) {
            int rows = Math.min(remainingRows, rowsInRowGroup);
            Map<Integer, ColumnStatistics> statistics = getRowGroupStatistics(types.get(0), columnIndexes, rowGroup);
            if (predicate.matches(rows, statistics)) {
                selectedRowGroups.add(rowGroup);
            }
            remainingRows -= rows;
        }
        return selectedRowGroups.build();
    }

    private static Map<Integer, ColumnStatistics> getRowGroupStatistics(OrcType rootStructType, Map<Integer, List<RowGroupIndex>> columnIndexes, int rowGroup)
    {
        checkNotNull(rootStructType, "rootStructType is null");
        checkArgument(rootStructType.getOrcTypeKind() == OrcTypeKind.STRUCT);
        checkNotNull(columnIndexes, "columnIndexes is null");
        checkArgument(rowGroup >= 0, "rowGroup is negative");

        ImmutableMap.Builder<Integer, ColumnStatistics> statistics = ImmutableMap.builder();
        for (int ordinal = 0; ordinal < rootStructType.getFieldCount(); ordinal++) {
            List<RowGroupIndex> rowGroupIndexes = columnIndexes.get(rootStructType.getFieldTypeIndex(ordinal));
            if (rowGroupIndexes != null) {
                statistics.put(ordinal, rowGroupIndexes.get(rowGroup).getColumnStatistics());
            }
        }
        return statistics.build();
    }

    private static boolean isIndexStream(Stream stream)
    {
        return stream.getStreamKind() == ROW_INDEX || stream.getStreamKind() == DICTIONARY_COUNT;
    }

    private static boolean isDictionary(Stream stream, ColumnEncodingKind columnEncoding)
    {
        return stream.getStreamKind() == DICTIONARY_DATA || (stream.getStreamKind() == LENGTH && (columnEncoding == DICTIONARY || columnEncoding == DICTIONARY_V2));
    }

    private static Map<StreamId, DiskRange> getDiskRanges(List<Stream> streams)
    {
        ImmutableMap.Builder<StreamId, DiskRange> streamDiskRanges = ImmutableMap.builder();
        long stripeOffset = 0;
        for (Stream stream : streams) {
            int streamLength = Ints.checkedCast(stream.getLength());
            streamDiskRanges.put(new StreamId(stream), new DiskRange(stripeOffset, streamLength));
            stripeOffset += streamLength;
        }
        return streamDiskRanges.build();
    }

    private static Set<Integer> getIncludedOrcColumns(List<OrcType> types, Set<Integer> includedColumns)
    {
        Set<Integer> includes = new LinkedHashSet<Integer>();

        OrcType root = types.get(0);
        for (int includedColumn : includedColumns) {
            includeOrcColumnsRecursive(types, includes, root.getFieldTypeIndex(includedColumn));
        }

        return includes;
    }

    private static void includeOrcColumnsRecursive(List<OrcType> types, Set<Integer> result, int typeId)
    {
        result.add(typeId);
        OrcType type = types.get(typeId);
        int children = type.getFieldCount();
        for (int i = 0; i < children; ++i) {
            includeOrcColumnsRecursive(types, result, type.getFieldTypeIndex(i));
        }
    }

    /**
     * Ceiling of integer division
     */
    private static int ceil(int dividend, int divisor)
    {
        return ((dividend + divisor) - 1) / divisor;
    }
}
