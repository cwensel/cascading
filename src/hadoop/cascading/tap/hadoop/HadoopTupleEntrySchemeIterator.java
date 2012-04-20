/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.hadoop;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tuple.TupleEntrySchemeIterator;
import cascading.util.CloseableIterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 *
 */
public class HadoopTupleEntrySchemeIterator extends TupleEntrySchemeIterator
  {
  public HadoopTupleEntrySchemeIterator( FlowProcess<JobConf> flowProcess, Scheme scheme, RecordReader recordReader )
    {
    this( flowProcess, scheme, new RecordReaderIterator( recordReader ) );
    }

  public HadoopTupleEntrySchemeIterator( FlowProcess<JobConf> flowProcess, Scheme scheme, Hfs parentTap ) throws IOException
    {
    this( flowProcess, scheme, new MultiRecordReaderIterator( flowProcess, parentTap ) );
    }

  public HadoopTupleEntrySchemeIterator( FlowProcess<JobConf> flowProcess, Scheme scheme, CloseableIterator<RecordReader> closeableIterator )
    {
    super( flowProcess, scheme, closeableIterator, flowProcess.getStringProperty( "cascading.source.path" ) );
    }
  }
