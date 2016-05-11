/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
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

package cascading.tuple;

import java.io.InputStream;

/**
 * The StreamComparator interface allows for two {@link InputStream} instances to be compared, typically bit-wise.
 * This is most useful when defining custom types to be stored in {@link Tuple} instances and the values will need
 * to be grouped on and/or secondary sorted (via {@link cascading.pipe.GroupBy} and/or {@link cascading.pipe.CoGroup})
 * and the underlying serialization implementation enables a useful bit-wise comparison without deserializing the custom
 * type into memory.
 * <p/>
 * Typically this interface is used to mark a {@link java.util.Comparator} as additionally
 * supporting the ability to compare raw streams in tandem with comparing Object instances.
 * Thus concrete implementations should implement this interface and the Comparator interface when being used
 * as a "grouping" or "sorting" field Comparator
 * <p/>
 * When used with Hadoop, a {@link cascading.tuple.hadoop.io.BufferedInputStream} is passed into the
 * {@link #compare(java.io.InputStream, java.io.InputStream)}
 * method. This class gives access to the underlying byte[] array so each individual byte need to be
 * {@link java.io.InputStream#read()}.
 * So it is useful to declare an implementation as
 * {@code public class YourCustomComparator implements StreamComparator&lt;BufferedInputStream>, Comparator&lt;YourCustomType>, Serializable}
 * <p/>
 * Note the method {@link cascading.tuple.hadoop.io.BufferedInputStream#skip(long)} will need to be called with the number
 * of bytes read from the underlying byte buffer before the compare() method returns.
 *
 * @param <T>
 */
public interface StreamComparator<T extends InputStream>
  {
  int compare( T lhsStream, T rhsStream );
  }
