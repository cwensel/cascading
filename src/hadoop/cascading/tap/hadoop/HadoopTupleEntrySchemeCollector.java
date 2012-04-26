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

import java.io.Closeable;
import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.tap.Tap;
import cascading.tap.hadoop.util.TimedOutputCollector;
import cascading.tuple.TupleEntrySchemeCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Class TapCollector is a kind of {@link cascading.tuple.TupleEntryCollector} that writes tuples to the resource managed by
 * a particular {@link cascading.tap.Tap} instance.
 */
public class HadoopTupleEntrySchemeCollector extends TupleEntrySchemeCollector<JobConf, OutputCollector>
  {

  private TimedOutputCollector timedOutputCollector;

  /**
   * Constructor TapCollector creates a new TapCollector instance.
   *
   * @param flowProcess
   * @param tap         of type Tap  @throws IOException when fails to initialize
   */
  public HadoopTupleEntrySchemeCollector( FlowProcess<JobConf> flowProcess, Tap<FlowProcess<JobConf>, JobConf, RecordReader, OutputCollector> tap ) throws IOException
    {
    this( flowProcess, tap, (String) null );
    }

  /**
   * Constructor TapCollector creates a new TapCollector instance.
   *
   * @param flowProcess
   * @param tap         of type Tap
   * @param prefix      of type String
   * @throws IOException when fails to initialize
   */
  public HadoopTupleEntrySchemeCollector( FlowProcess<JobConf> flowProcess, Tap<FlowProcess<JobConf>, JobConf, RecordReader, OutputCollector> tap, String prefix ) throws IOException
    {
    super( flowProcess, tap.getScheme(), new TapOutputCollector( flowProcess, tap, prefix ), tap.getIdentifier() );
    }

  public HadoopTupleEntrySchemeCollector( FlowProcess<JobConf> flowProcess, Tap<FlowProcess<JobConf>, JobConf, RecordReader, OutputCollector> tap, OutputCollector outputCollector )
    {
    super( flowProcess, tap.getScheme(), outputCollector, tap.getIdentifier() );
    }

  @Override
  protected OutputCollector wrapOutput( OutputCollector outputCollector )
    {
    if( timedOutputCollector == null )
      timedOutputCollector = new TimedOutputCollector( getFlowProcess(), SliceCounters.Write_Duration );

    timedOutputCollector.setOutputCollector( outputCollector );

    return timedOutputCollector;
    }

  @Override
  public void close()
    {
    try
      {
      if( getOutput() instanceof Closeable )
        ( (Closeable) getOutput() ).close();
      }
    catch( IOException exception )
      {
      // do nothing
      }
    finally
      {
      super.close();
      }
    }
  }
