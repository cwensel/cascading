/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

/**
 * A wrapper class for a record reader that handles a single file split. It delegates most of the
 * methods to the wrapped instance. We need this wrapper to satisfy the constructor requirement to
 * be used with hadoop's CombineFileRecordReader class.
 *
 * @see org.apache.hadoop.mapred.lib.CombineFileRecordReader
 * @see org.apache.hadoop.mapred.lib.CombineFileInputFormat
 */
public class CombineFileRecordReaderWrapper<K, V> implements RecordReader<K, V>
  {
  /** property that indicates how individual input format is to be interpreted */
  public static final String INDIVIDUAL_INPUT_FORMAT = "cascading.individual.input.format";

  private final RecordReader<K, V> delegate;

  // this constructor signature is required by CombineFileRecordReader
  public CombineFileRecordReaderWrapper( CombineFileSplit split, Configuration conf, Reporter reporter, Integer idx ) throws Exception
    {
    FileSplit fileSplit = new FileSplit(
      split.getPath( idx ),
      split.getOffset( idx ),
      split.getLength( idx ),
      split.getLocations()
    );

    Class<?> clz = conf.getClass( INDIVIDUAL_INPUT_FORMAT, null );
    FileInputFormat<K, V> inputFormat = (FileInputFormat<K, V>) clz.newInstance();

    delegate = inputFormat.getRecordReader( fileSplit, (JobConf) conf, reporter );
    }

  public boolean next( K key, V value ) throws IOException
    {
    return delegate.next( key, value );
    }

  public K createKey()
    {
    return delegate.createKey();
    }

  public V createValue()
    {
    return delegate.createValue();
    }

  public long getPos() throws IOException
    {
    return delegate.getPos();
    }

  public void close() throws IOException
    {
    delegate.close();
    }

  public float getProgress() throws IOException
    {
    return delegate.getProgress();
    }
  }