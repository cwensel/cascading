/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.platform.hadoop;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

/**
 *
 */
public class HadoopFailScheme extends TextLine
  {
  boolean sourceFired = false;
  boolean sinkFired = false;

  public HadoopFailScheme()
    {
    }

  public HadoopFailScheme( Fields sourceFields )
    {
    super( sourceFields );
    }

  @Override
  public boolean source( FlowProcess<? extends Configuration> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException
    {
    if( !sourceFired )
      {
      sourceFired = true;
      throw new TapException( "fail", new Tuple( "bad data" ) );
      }

    return super.source( flowProcess, sourceCall );
    }

  @Override
  public void sink( FlowProcess<? extends Configuration> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
    {
    if( !sinkFired )
      {
      sinkFired = true;
      throw new TapException( "fail", new Tuple( "bad data" ) );
      }

    super.sink( flowProcess, sinkCall );
    }
  }
