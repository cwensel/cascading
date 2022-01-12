/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

import cascading.flow.FlowProcess;
import cascading.flow.SliceCounters;
import cascading.tap.Tap;
import cascading.tap.hadoop.util.MeasuredOutputCollector;
import cascading.tuple.TupleEntrySchemeCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Class HadoopTupleEntrySchemeCollector is a kind of {@link cascading.tuple.TupleEntryCollector} that writes tuples to the resource managed by
 * a particular {@link cascading.tap.Tap} instance.
 */
public class HadoopTupleEntrySchemeCollector extends TupleEntrySchemeCollector<Configuration, OutputCollector>
  {
  private MeasuredOutputCollector measuredOutputCollector;

  public HadoopTupleEntrySchemeCollector( FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap ) throws IOException
    {
    super( flowProcess, tap, tap.getScheme(), makeCollector( flowProcess, tap, null ), tap.getIdentifier() );
    }

  public HadoopTupleEntrySchemeCollector( FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap, OutputCollector outputCollector ) throws IOException
    {
    super( flowProcess, tap, tap.getScheme(), makeCollector( flowProcess, tap, outputCollector ), tap.getIdentifier() );
    }

  private static OutputCollector makeCollector( FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap, OutputCollector outputCollector ) throws IOException
    {
    if( outputCollector != null )
      return outputCollector;

    return new TapOutputCollector( flowProcess, tap );
    }

  @Override
  protected OutputCollector<?, ?> wrapOutput( OutputCollector outputCollector )
    {
    if( measuredOutputCollector == null )
      measuredOutputCollector = new MeasuredOutputCollector( getFlowProcess(), SliceCounters.Write_Duration );

    measuredOutputCollector.setOutputCollector( super.wrapOutput( outputCollector ) );

    return measuredOutputCollector;
    }
  }
