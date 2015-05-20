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

import javax.annotation.Nullable;
import java.io.IOException;

public class MissingStreamSource<S extends ValueStream<?>> implements StreamSource<S>
{
    private final Class<S> streamType;

    public static <S extends ValueStream<?>> StreamSource<S> missingStreamSource(Class<S> streamType)
    {
        return new MissingStreamSource<S>(streamType);
    }

    private MissingStreamSource(Class<S> streamType)
    {
        this.streamType = streamType;
    }

    @Override
    public Class<S> getStreamType()
    {
        return streamType;
    }

    @Nullable
    @Override
    public S openStream()
            throws IOException
    {
        return null;
    }
}
