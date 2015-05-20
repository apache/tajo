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
package org.apache.tajo.storage.thirdparty.orc.metadata;

import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.OrcProto.ColumnEncoding.Kind;
import com.google.common.base.Function;
import org.apache.tajo.storage.thirdparty.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import org.apache.tajo.storage.thirdparty.orc.metadata.OrcType.OrcTypeKind;
import org.apache.tajo.storage.thirdparty.orc.metadata.Stream.StreamKind;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.protobuf.CodedInputStream;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.apache.tajo.storage.thirdparty.orc.metadata.CompressionKind.*;
import static org.apache.tajo.storage.thirdparty.orc.metadata.OrcMetadataReader.getMaxSlice;
import static org.apache.tajo.storage.thirdparty.orc.metadata.OrcMetadataReader.getMinSlice;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class DwrfMetadataReader
        implements MetadataReader
{
    @Override
    public PostScript readPostScript(byte[] data, int offset, int length)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(data, offset, length);
        OrcProto.PostScript postScript = OrcProto.PostScript.parseFrom(input);

        return new PostScript(
                ImmutableList.<Integer>of(),
                postScript.getFooterLength(),
                0,
                toCompression(postScript.getCompression()),
                postScript.getCompressionBlockSize());
    }

    @Override
    public Metadata readMetadata(InputStream inputStream)
            throws IOException
    {
        return new Metadata(ImmutableList.<StripeStatistics>of());
    }

    @Override
    public Footer readFooter(InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.Footer footer = OrcProto.Footer.parseFrom(input);
        return new Footer(
                footer.getNumberOfRows(),
                footer.getRowIndexStride(),
                toStripeInformation(footer.getStripesList()),
                toType(footer.getTypesList()),
                toColumnStatistics(footer.getStatisticsList(), false));
    }

    private static List<StripeInformation> toStripeInformation(List<OrcProto.StripeInformation> types)
    {
        // Modifying for JDK 1.6
        // return ImmutableList.copyOf(Iterables.transform(types, DwrfMetadataReader::toStripeInformation));
        return ImmutableList.copyOf(Iterables.transform(types, new Function<OrcProto.StripeInformation, StripeInformation>() {
            @Nullable
            @Override
            public StripeInformation apply(OrcProto.StripeInformation stripeInformation) {
                return toStripeInformation(stripeInformation);
            }
        }));
    }

    private static StripeInformation toStripeInformation(OrcProto.StripeInformation stripeInformation)
    {
        return new StripeInformation(
                Ints.checkedCast(stripeInformation.getNumberOfRows()),
                stripeInformation.getOffset(),
                stripeInformation.getIndexLength(),
                stripeInformation.getDataLength(),
                stripeInformation.getFooterLength());
    }

    @Override
    public StripeFooter readStripeFooter(List<OrcType> types, InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.StripeFooter stripeFooter = OrcProto.StripeFooter.parseFrom(input);
        return new StripeFooter(toStream(stripeFooter.getStreamsList()), toColumnEncoding(types, stripeFooter.getColumnsList()));
    }

    private static Stream toStream(OrcProto.Stream stream)
    {
        return new Stream(stream.getColumn(), toStreamKind(stream.getKind()), Ints.checkedCast(stream.getLength()), stream.getUseVInts());
    }

    private static List<Stream> toStream(List<OrcProto.Stream> streams)
    {
        // Modifying for JDK 1.6
        // return ImmutableList.copyOf(Iterables.transform(streams, DwrfMetadataReader::toStream));
        return ImmutableList.copyOf(Iterables.transform(streams, new Function<OrcProto.Stream, Stream>() {
            @Nullable
            @Override
            public Stream apply(@Nullable OrcProto.Stream stream) {
                return toStream(stream);
            }
        }));
    }

    private static ColumnEncoding toColumnEncoding(OrcTypeKind type, OrcProto.ColumnEncoding columnEncoding)
    {
        return new ColumnEncoding(toColumnEncodingKind(type, columnEncoding.getKind()), columnEncoding.getDictionarySize());
    }

    private static List<ColumnEncoding> toColumnEncoding(List<OrcType> types, List<OrcProto.ColumnEncoding> columnEncodings)
    {
        checkArgument(types.size() == columnEncodings.size());

        ImmutableList.Builder<ColumnEncoding> encodings = ImmutableList.builder();
        for (int i = 0; i < types.size(); i++) {
            OrcType type = types.get(i);
            encodings.add(toColumnEncoding(type.getOrcTypeKind(), columnEncodings.get(i)));
        }
        return encodings.build();
    }

    @Override
    public List<RowGroupIndex> readRowIndexes(InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.RowIndex rowIndex = OrcProto.RowIndex.parseFrom(input);

        // Modifying for JDK 1.6
        // return ImmutableList.copyOf(Iterables.transform(rowIndex.getEntryList(), DwrfMetadataReader::toRowGroupIndex));
        return ImmutableList.copyOf(Iterables.transform(rowIndex.getEntryList(), new Function<OrcProto.RowIndexEntry, RowGroupIndex>() {
            @Nullable
            @Override
            public RowGroupIndex apply(@Nullable OrcProto.RowIndexEntry rowIndexEntry) {
                return toRowGroupIndex(rowIndexEntry);
            }
        }));
    }

    private static RowGroupIndex toRowGroupIndex(OrcProto.RowIndexEntry rowIndexEntry)
    {
        List<Long> positionsList = rowIndexEntry.getPositionsList();
        ImmutableList.Builder<Integer> positions = ImmutableList.builder();
        for (int index = 0; index < positionsList.size(); index++) {
            long longPosition = positionsList.get(index);
            int intPosition = (int) longPosition;

            checkState(intPosition == longPosition, "Expected checkpoint position %s, to be an integer", index);

            positions.add(intPosition);
        }
        return new RowGroupIndex(positions.build(), toColumnStatistics(rowIndexEntry.getStatistics(), true));
    }

    private static List<ColumnStatistics> toColumnStatistics(List<OrcProto.ColumnStatistics> columnStatistics, final boolean isRowGroup)
    {
        if (columnStatistics == null) {
            return ImmutableList.of();
        }

        // Modifying for JDK 1.6
        // return ImmutableList.copyOf(Iterables.transform(columnStatistics, statistics -> toColumnStatistics(statistics, isRowGroup)));
        return ImmutableList.copyOf(Iterables.transform(columnStatistics, new Function<OrcProto.ColumnStatistics, ColumnStatistics>() {
            @Nullable
            @Override
            public ColumnStatistics apply(@Nullable OrcProto.ColumnStatistics columnStatistics) {
                return toColumnStatistics(columnStatistics, isRowGroup);
            }
        }));
    }

    private static ColumnStatistics toColumnStatistics(OrcProto.ColumnStatistics statistics, boolean isRowGroup)
    {
        return new ColumnStatistics(
                statistics.getNumberOfValues(),
                toBooleanStatistics(statistics.getBucketStatistics()),
                toIntegerStatistics(statistics.getIntStatistics()),
                toDoubleStatistics(statistics.getDoubleStatistics()),
                toStringStatistics(statistics.getStringStatistics(), isRowGroup),
                null);
    }

    private static BooleanStatistics toBooleanStatistics(OrcProto.BucketStatistics bucketStatistics)
    {
        if (bucketStatistics.getCountCount() == 0) {
            return null;
        }

        return new BooleanStatistics(bucketStatistics.getCount(0));
    }

    private static IntegerStatistics toIntegerStatistics(OrcProto.IntegerStatistics integerStatistics)
    {
        if (!integerStatistics.hasMinimum() && !integerStatistics.hasMaximum()) {
            return null;
        }

        return new IntegerStatistics(
                integerStatistics.hasMinimum() ? integerStatistics.getMinimum() : null,
                integerStatistics.hasMaximum() ? integerStatistics.getMaximum() : null);
    }

    private static DoubleStatistics toDoubleStatistics(OrcProto.DoubleStatistics doubleStatistics)
    {
        if (!doubleStatistics.hasMinimum() && !doubleStatistics.hasMaximum()) {
            return null;
        }

        // if either min, max, or sum is NaN, ignore the stat
        if ((doubleStatistics.hasMinimum() && Double.isNaN(doubleStatistics.getMinimum())) ||
                (doubleStatistics.hasMaximum() && Double.isNaN(doubleStatistics.getMaximum())) ||
                (doubleStatistics.hasSum() && Double.isNaN(doubleStatistics.getSum()))) {
            return null;
        }

        return new DoubleStatistics(
                doubleStatistics.hasMinimum() ? doubleStatistics.getMinimum() : null,
                doubleStatistics.hasMaximum() ? doubleStatistics.getMaximum() : null);
    }

    private static StringStatistics toStringStatistics(OrcProto.StringStatistics stringStatistics, boolean isRowGroup)
    {
        // TODO remove this when string statistics in ORC are fixed https://issues.apache.org/jira/browse/HIVE-8732
        if (!isRowGroup) {
            return null;
        }

        if (!stringStatistics.hasMinimum() && !stringStatistics.hasMaximum()) {
            return null;
        }

        Slice minimum = stringStatistics.hasMinimum() ? getMinSlice(stringStatistics.getMinimum()) : null;
        Slice maximum = stringStatistics.hasMaximum() ? getMaxSlice(stringStatistics.getMaximum()) : null;

        return new StringStatistics(minimum, maximum);
    }

    private static OrcType toType(OrcProto.Type type)
    {
        return new OrcType(toTypeKind(type.getKind()), type.getSubtypesList(), type.getFieldNamesList());
    }

    private static List<OrcType> toType(List<OrcProto.Type> types)
    {
        // Modifying for JDK 1.6
        // return ImmutableList.copyOf(Iterables.transform(types, DwrfMetadataReader::toType));
        return ImmutableList.copyOf(Iterables.transform(types, new Function<OrcProto.Type, OrcType>() {
            @Nullable
            @Override
            public OrcType apply(@Nullable OrcProto.Type type) {
                return toType(type);
            }
        }));
    }

    private static OrcTypeKind toTypeKind(OrcProto.Type.Kind kind)
    {
        switch (kind) {
            case BOOLEAN:
                return OrcTypeKind.BOOLEAN;
            case BYTE:
                return OrcTypeKind.BYTE;
            case SHORT:
                return OrcTypeKind.SHORT;
            case INT:
                return OrcTypeKind.INT;
            case LONG:
                return OrcTypeKind.LONG;
            case FLOAT:
                return OrcTypeKind.FLOAT;
            case DOUBLE:
                return OrcTypeKind.DOUBLE;
            case STRING:
                return OrcTypeKind.STRING;
            case BINARY:
                return OrcTypeKind.BINARY;
            case TIMESTAMP:
                return OrcTypeKind.TIMESTAMP;
            case LIST:
                return OrcTypeKind.LIST;
            case MAP:
                return OrcTypeKind.MAP;
            case STRUCT:
                return OrcTypeKind.STRUCT;
            case UNION:
                return OrcTypeKind.UNION;
            default:
                throw new IllegalArgumentException(kind + " data type not implemented yet");
        }
    }

    private static StreamKind toStreamKind(OrcProto.Stream.Kind kind)
    {
        switch (kind) {
            case PRESENT:
                return StreamKind.PRESENT;
            case DATA:
                return StreamKind.DATA;
            case LENGTH:
                return StreamKind.LENGTH;
            case DICTIONARY_DATA:
                return StreamKind.DICTIONARY_DATA;
            case DICTIONARY_COUNT:
                return StreamKind.DICTIONARY_COUNT;
            case NANO_DATA:
                return StreamKind.SECONDARY;
            case ROW_INDEX:
                return StreamKind.ROW_INDEX;
            case IN_DICTIONARY:
                return StreamKind.IN_DICTIONARY;
            case STRIDE_DICTIONARY:
                return StreamKind.ROW_GROUP_DICTIONARY;
            case STRIDE_DICTIONARY_LENGTH:
                return StreamKind.ROW_GROUP_DICTIONARY_LENGTH;
            default:
                throw new IllegalArgumentException(kind + " stream type not implemented yet");
        }
    }

    private static ColumnEncodingKind toColumnEncodingKind(OrcTypeKind type, Kind kind)
    {
        switch (kind) {
            case DIRECT:
                if (type == OrcTypeKind.SHORT || type == OrcTypeKind.INT || type == OrcTypeKind.LONG) {
                    return ColumnEncodingKind.DWRF_DIRECT;
                }
                else {
                    return ColumnEncodingKind.DIRECT;
                }
            case DICTIONARY:
                return ColumnEncodingKind.DICTIONARY;
            default:
                throw new IllegalArgumentException(kind + " stream encoding not implemented yet");
        }
    }

    private static CompressionKind toCompression(OrcProto.CompressionKind compression)
    {
        switch (compression) {
            case NONE:
                return UNCOMPRESSED;
            case ZLIB:
                return ZLIB;
            case SNAPPY:
                return SNAPPY;
            default:
                throw new IllegalArgumentException(compression + " compression not implemented yet");
        }
    }
}
