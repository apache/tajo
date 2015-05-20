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
package org.apache.tajo.storage.thirdparty.orc.stream;

import com.google.common.base.MoreObjects;
import org.apache.tajo.storage.thirdparty.orc.OrcCorruptionException;
import org.apache.tajo.storage.thirdparty.orc.metadata.CompressionKind;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.iq80.snappy.Snappy;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.tajo.storage.thirdparty.orc.OrcCorruptionException.verifyFormat;
import static org.apache.tajo.storage.thirdparty.orc.checkpoint.InputStreamCheckpoint.*;
import static org.apache.tajo.storage.thirdparty.orc.metadata.CompressionKind.*;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class OrcInputStream
        extends InputStream
{
    public static final int BLOCK_HEADER_SIZE = 3;

    private final String source;
    private final BasicSliceInput compressedSliceInput;
    private final CompressionKind compressionKind;
    private final int bufferSize;

    private int currentCompressedBlockOffset;
    private BasicSliceInput current;

    private Slice buffer;

    public OrcInputStream(String source, BasicSliceInput sliceInput, CompressionKind compressionKind, int bufferSize)
    {
        this.source = checkNotNull(source, "source is null");

        checkNotNull(sliceInput, "sliceInput is null");

        this.compressionKind = checkNotNull(compressionKind, "compressionKind is null");
        this.bufferSize = bufferSize;

        if (compressionKind == UNCOMPRESSED) {
            this.current = sliceInput;
            this.compressedSliceInput = EMPTY_SLICE.getInput();
        }
        else {
            checkArgument(compressionKind == SNAPPY || compressionKind == ZLIB, "%s compression not supported", compressionKind);
            this.compressedSliceInput = checkNotNull(sliceInput, "compressedSliceInput is null");
            this.current = EMPTY_SLICE.getInput();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        current = null;
    }

    @Override
    public int available()
            throws IOException
    {
        if (current == null) {
            return 0;
        }
        return current.available();
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    @Override
    public int read()
            throws IOException
    {
        if (current == null) {
            return -1;
        }

        int result = current.read();
        if (result != -1) {
            return result;
        }

        advance();
        return read();
    }

    @Override
    public int read(byte[] b, int off, int length)
            throws IOException
    {
        if (current == null) {
            return -1;
        }

        if (!current.isReadable()) {
            advance();
            if (current == null) {
                return -1;
            }
        }

        return current.read(b, off, length);
    }

    public long getCheckpoint()
    {
        // if the decompressed buffer is empty, return a checkpoint starting at the next block
        if (current == null || (current.position() == 0 && current.available() == 0)) {
            return createInputStreamCheckpoint(compressedSliceInput.position(), 0);
        }
        // otherwise return a checkpoint at the last compressed block read and the current position in the buffer
        return createInputStreamCheckpoint(currentCompressedBlockOffset, current.position());
    }

    public boolean seekToCheckpoint(long checkpoint)
            throws IOException
    {
        int compressedBlockOffset = decodeCompressedBlockOffset(checkpoint);
        int decompressedOffset = decodeDecompressedOffset(checkpoint);
        boolean discardedBuffer;
        if (compressedBlockOffset != currentCompressedBlockOffset) {
            verifyFormat(compressionKind != UNCOMPRESSED, "Reset stream has a compressed block offset but stream is not compressed");
            compressedSliceInput.setPosition(compressedBlockOffset);
            current = EMPTY_SLICE.getInput();
            discardedBuffer = true;
        }
        else {
            discardedBuffer = false;
        }

        if (decompressedOffset != current.position()) {
            current.setPosition(0);
            if (current.available() < decompressedOffset)  {
                decompressedOffset -= current.available();
                advance();
            }
            current.setPosition(decompressedOffset);
        }
        return discardedBuffer;
    }

    @Override
    public long skip(long n)
            throws IOException
    {
        if (current == null || n <= 0) {
            return -1;
        }

        long result = current.skip(n);
        if (result != 0) {
            return result;
        }
        if (read() == -1) {
            return 0;
        }
        return 1 + current.skip(n - 1);
    }

    // This comes from the Apache Hive ORC code
    private void advance()
            throws IOException
    {
        if (compressedSliceInput == null || compressedSliceInput.available() == 0) {
            current = null;
            return;
        }

        // 3 byte header
        // NOTE: this must match BLOCK_HEADER_SIZE
        currentCompressedBlockOffset = compressedSliceInput.position();
        int b0 = compressedSliceInput.readUnsignedByte();
        int b1 = compressedSliceInput.readUnsignedByte();
        int b2 = compressedSliceInput.readUnsignedByte();

        boolean isUncompressed = (b0 & 0x01) == 1;
        int chunkLength = (b2 << 15) | (b1 << 7) | (b0 >>> 1);

        Slice chunk = compressedSliceInput.readSlice(chunkLength);

        if (isUncompressed) {
            current = chunk.getInput();
        }
        else {
            if (buffer == null) {
                buffer = Slices.allocate(bufferSize);
            }

            int uncompressedSize;
            if (compressionKind == ZLIB) {
                uncompressedSize = decompressZip(chunk, buffer);
            }
            else {
                uncompressedSize = decompressSnappy(chunk, buffer);
            }

            current = buffer.slice(0, uncompressedSize).getInput();
        }
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("source", source)
                .add("compressedOffset", compressedSliceInput.position())
                .add("uncompressedOffset", current == null ? null : current.position())
                .add("compression", compressionKind)
                .toString();
    }

    // This comes from the Apache Hive ORC code
    private static int decompressZip(Slice in, Slice buffer)
            throws IOException
    {
        byte[] outArray = (byte[]) buffer.getBase();
        int outOffset = 0;

        byte[] inArray = (byte[]) in.getBase();
        int inOffset = (int) (in.getAddress() - ARRAY_BYTE_BASE_OFFSET);
        int inLength = in.length();

        Inflater inflater = new Inflater(true);
        inflater.setInput(inArray, inOffset, inLength);
        while (!(inflater.finished() || inflater.needsDictionary() || inflater.needsInput())) {
            try {
                int count = inflater.inflate(outArray, outOffset, outArray.length - outOffset);
                outOffset += count;
            }
            catch (DataFormatException e) {
                throw new OrcCorruptionException(e, "Invalid compressed stream");
            }
        }
        inflater.end();
        return outOffset;
    }

    private static int decompressSnappy(Slice in, Slice buffer)
            throws IOException
    {
        byte[] outArray = (byte[]) buffer.getBase();

        byte[] inArray = (byte[]) in.getBase();
        int inOffset = (int) (in.getAddress() - ARRAY_BYTE_BASE_OFFSET);
        int inLength = in.length();

        return Snappy.uncompress(inArray, inOffset, inLength, outArray, 0);
    }
}
