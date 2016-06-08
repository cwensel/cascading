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

package cascading.tap.hadoop.io;

import java.io.IOException;

import cascading.util.SingleValueCloseableIterator;
import org.apache.hadoop.mapred.RecordReader;

/**
 * RecordReaderIterator is a utility class for handing off a single {@link RecordReader} instance
 * via the {@link java.util.Iterator}.
 * <p/>
 * This class is frequently used with the {@link cascading.tuple.TupleEntrySchemeIterator} where there is only
 * one RecordReader to iterate values over.
 */
public class RecordReaderIterator extends SingleValueCloseableIterator<RecordReader>
  {
  public RecordReaderIterator( RecordReader reader )
    {
    super( reader );
    }

  @Override
  public void close() throws IOException
    {
    getCloseableInput().close();
    }
  }
