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

import com.facebook.presto.hive.shaded.com.google.protobuf.CodedInputStream;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import org.apache.tajo.storage.thirdparty.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import org.apache.tajo.storage.thirdparty.orc.metadata.OrcType.OrcTypeKind;
import org.apache.tajo.storage.thirdparty.orc.metadata.Stream.StreamKind;
import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndexEntry;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.tajo.storage.thirdparty.orc.metadata.CompressionKind.*;

public class OrcMetadataReader
        implements MetadataReader
{
    @Override
    public PostScript readPostScript(byte[] data, int offset, int length)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(data, offset, length);
        OrcProto.PostScript postScript = OrcProto.PostScript.parseFrom(input);

        return new PostScript(
                postScript.getVersionList(),
                postScript.getFooterLength(),
                postScript.getMetadataLength(),
                toCompression(postScript.getCompression()),
                postScript.getCompressionBlockSize());
    }

    @Override
    public Metadata readMetadata(InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.Metadata metadata = OrcProto.Metadata.parseFrom(input);
        return new Metadata(toStripeStatistics(metadata.getStripeStatsList()));
    }

    private static List<StripeStatistics> toStripeStatistics(List<OrcProto.StripeStatistics> types)
    {
        return ImmutableList.copyOf(Iterables.transform(types, new Function<OrcProto.StripeStatistics, StripeStatistics>()
        {
            @Override
            public StripeStatistics apply(OrcProto.StripeStatistics type)
            {
                return toStripeStatistics(type);
            }
        }));
    }

    private static StripeStatistics toStripeStatistics(OrcProto.StripeStatistics stripeStatistics)
    {
        return new StripeStatistics(toColumnStatistics(stripeStatistics.getColStatsList(), false));
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
        return ImmutableList.copyOf(Iterables.transform(types, new Function<OrcProto.StripeInformation, StripeInformation>()
        {
            @Override
            public StripeInformation apply(OrcProto.StripeInformation type)
            {
                return toStripeInformation(type);
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
        return new StripeFooter(toStream(stripeFooter.getStreamsList()), toColumnEncoding(stripeFooter.getColumnsList()));
    }

    private static Stream toStream(OrcProto.Stream stream)
    {
        return new Stream(stream.getColumn(), toStreamKind(stream.getKind()), Ints.checkedCast(stream.getLength()), true);
    }

    private static List<Stream> toStream(List<OrcProto.Stream> streams)
    {
        return ImmutableList.copyOf(Iterables.transform(streams, new Function<OrcProto.Stream, Stream>()
        {
            @Override
            public Stream apply(OrcProto.Stream stream)
            {
                return toStream(stream);
            }
        }));
    }

    private static ColumnEncoding toColumnEncoding(OrcProto.ColumnEncoding columnEncoding)
    {
        return new ColumnEncoding(toColumnEncodingKind(columnEncoding.getKind()), columnEncoding.getDictionarySize());
    }

    private static List<ColumnEncoding> toColumnEncoding(List<OrcProto.ColumnEncoding> columnEncodings)
    {
        return ImmutableList.copyOf(Iterables.transform(columnEncodings, new Function<OrcProto.ColumnEncoding, ColumnEncoding>()
        {
            @Override
            public ColumnEncoding apply(OrcProto.ColumnEncoding columnEncoding)
            {
                return toColumnEncoding(columnEncoding);
            }
        }));
    }

    @Override
    public List<RowGroupIndex> readRowIndexes(InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.RowIndex rowIndex = OrcProto.RowIndex.parseFrom(input);
        return ImmutableList.copyOf(Iterables.transform(rowIndex.getEntryList(), new Function<RowIndexEntry, RowGroupIndex>()
        {
            @Override
            public RowGroupIndex apply(RowIndexEntry rowIndexEntry)
            {
                return toRowGroupIndex(rowIndexEntry);
            }
        }));
    }

    private static RowGroupIndex toRowGroupIndex(RowIndexEntry rowIndexEntry)
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

    private static ColumnStatistics toColumnStatistics(OrcProto.ColumnStatistics statistics, boolean isRowGroup)
    {
        return new ColumnStatistics(
                statistics.getNumberOfValues(),
                toBooleanStatistics(statistics.getBucketStatistics()),
                toIntegerStatistics(statistics.getIntStatistics()),
                toDoubleStatistics(statistics.getDoubleStatistics()),
                toStringStatistics(statistics.getStringStatistics(), isRowGroup),
                toDateStatistics(statistics.getDateStatistics(), isRowGroup));
    }

    private static List<ColumnStatistics> toColumnStatistics(List<OrcProto.ColumnStatistics> columnStatistics, final boolean isRowGroup)
    {
        if (columnStatistics == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(Iterables.transform(columnStatistics, new Function<OrcProto.ColumnStatistics, ColumnStatistics>()
        {
            @Override
            public ColumnStatistics apply(OrcProto.ColumnStatistics columnStatistics)
            {
                return toColumnStatistics(columnStatistics, isRowGroup);
            }
        }));
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

        // TODO remove this when double statistics are changed to correctly deal with NaNs
        // if either min or max is NaN, ignore the stat
        if ((doubleStatistics.hasMinimum() && Double.isNaN(doubleStatistics.getMinimum())) ||
                (doubleStatistics.hasMaximum() && Double.isNaN(doubleStatistics.getMaximum()))) {
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

        return new StringStatistics(
                stringStatistics.hasMinimum() ? stringStatistics.getMinimum() : null,
                stringStatistics.hasMaximum() ? stringStatistics.getMaximum() : null);
    }

    private static DateStatistics toDateStatistics(OrcProto.DateStatistics dateStatistics, boolean isRowGroup)
    {
        // TODO remove this when date statistics in ORC are fixed https://issues.apache.org/jira/browse/HIVE-8732
        if (!isRowGroup) {
            return null;
        }

        if (!dateStatistics.hasMinimum() && !dateStatistics.hasMaximum()) {
            return null;
        }

        // temporarily disable string statistics until we figure out the implications of how UTF-16
        // strings are compared when they contain surrogate pairs and replacement characters
        if (true) {
            return null;
        }

        return new DateStatistics(
                dateStatistics.hasMinimum() ? dateStatistics.getMinimum() : null,
                dateStatistics.hasMaximum() ? dateStatistics.getMaximum() : null);
    }

    private static OrcType toType(OrcProto.Type type)
    {
        return new OrcType(toTypeKind(type.getKind()), type.getSubtypesList(), type.getFieldNamesList());
    }

    private static List<OrcType> toType(List<OrcProto.Type> types)
    {
        return ImmutableList.copyOf(Iterables.transform(types, new Function<OrcProto.Type, OrcType>()
        {
            @Override
            public OrcType apply(OrcProto.Type type)
            {
                return toType(type);
            }
        }));
    }

    private static OrcTypeKind toTypeKind(OrcProto.Type.Kind typeKind)
    {
        switch (typeKind) {
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
            case DECIMAL:
                return OrcTypeKind.DECIMAL;
            case DATE:
                return OrcTypeKind.DATE;
            case VARCHAR:
                return OrcTypeKind.VARCHAR;
            case CHAR:
                return OrcTypeKind.CHAR;
            default:
                throw new IllegalStateException(typeKind + " stream type not implemented yet");
        }
    }

    private static StreamKind toStreamKind(OrcProto.Stream.Kind streamKind)
    {
        switch (streamKind) {
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
            case SECONDARY:
                return StreamKind.SECONDARY;
            case ROW_INDEX:
                return StreamKind.ROW_INDEX;
            default:
                throw new IllegalStateException(streamKind + " stream type not implemented yet");
        }
    }

    private static ColumnEncodingKind toColumnEncodingKind(OrcProto.ColumnEncoding.Kind columnEncodingKind)
    {
        switch (columnEncodingKind) {
            case DIRECT:
                return ColumnEncodingKind.DIRECT;
            case DIRECT_V2:
                return ColumnEncodingKind.DIRECT_V2;
            case DICTIONARY:
                return ColumnEncodingKind.DICTIONARY;
            case DICTIONARY_V2:
                return ColumnEncodingKind.DICTIONARY_V2;
            default:
                throw new IllegalStateException(columnEncodingKind + " stream encoding not implemented yet");
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
                throw new IllegalStateException(compression + " compression not implemented yet");
        }
    }
}
