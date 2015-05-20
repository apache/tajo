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
package org.apache.tajo.storage.thirdparty.orc.reader;

import org.apache.tajo.storage.thirdparty.orc.OrcCorruptionException;

final class OrcReaderUtils
{
    private OrcReaderUtils()
    {
    }

    public static <T> T castOrcVector(Object vector, Class<T> type)
            throws OrcCorruptionException
    {
        if (!type.isInstance(vector)) {
            throw new OrcCorruptionException("Expected %s, but got %s", type.getSimpleName(), vector.getClass().getName());
        }
        return type.cast(vector);
    }
}
