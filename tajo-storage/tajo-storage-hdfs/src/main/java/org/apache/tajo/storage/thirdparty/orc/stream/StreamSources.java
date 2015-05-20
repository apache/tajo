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

import org.apache.tajo.storage.thirdparty.orc.StreamDescriptor;
import org.apache.tajo.storage.thirdparty.orc.StreamId;
import org.apache.tajo.storage.thirdparty.orc.metadata.Stream.StreamKind;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.Map;

import static org.apache.tajo.storage.thirdparty.orc.stream.MissingStreamSource.missingStreamSource;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class StreamSources
{
    private final Map<StreamId, StreamSource<?>> streamSources;

    public StreamSources(Map<StreamId, StreamSource<?>> streamSources)
    {
        this.streamSources = ImmutableMap.copyOf(checkNotNull(streamSources, "streamSources is null"));
    }

    @Nonnull
    public <S extends ValueStream<?>> StreamSource<S> getStreamSource(StreamDescriptor streamDescriptor, StreamKind streamKind, Class<S> streamType)
    {
        checkNotNull(streamDescriptor, "streamDescriptor is null");
        checkNotNull(streamType, "streamType is null");

        StreamSource<?> streamSource = streamSources.get(new StreamId(streamDescriptor.getStreamId(), streamKind));
        if (streamSource == null) {
            streamSource = missingStreamSource(streamType);
        }

        checkArgument(streamType.isAssignableFrom(streamSource.getStreamType()),
                "%s must be of type %s, not %s",
                streamDescriptor,
                streamType.getName(),
                streamSource.getStreamType().getName());

        return (StreamSource<S>) streamSource;
    }
}
