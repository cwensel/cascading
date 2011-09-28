/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.hadoop;

import java.io.IOException;

import cascading.CascadingException;
import cascading.flow.FlowException;
import cascading.flow.FlowSession;
import cascading.flow.stream.SourceStage;
import cascading.tap.Tap;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/** Class FlowMapper is the Hadoop Mapper implementation. */
public class FlowMapper implements MapRunnable
  {
  private HadoopMapStreamGraph streamGraph;
  /** Field currentProcess */
  private HadoopFlowProcess currentProcess;


  /** Constructor FlowMapper creates a new FlowMapper instance. */
  public FlowMapper()
    {
    }

  @Override
  public void configure( JobConf jobConf )
    {
    try
      {
      HadoopUtil.initLog4j( jobConf );

      currentProcess = new HadoopFlowProcess( new FlowSession(), jobConf, true );

      HadoopFlowStep step = (HadoopFlowStep) HadoopUtil.deserializeBase64( jobConf.getRaw( "cascading.flow.step" ) );
      Tap source = (Tap) HadoopUtil.deserializeBase64( jobConf.getRaw( "cascading.step.source" ) );

      streamGraph = new HadoopMapStreamGraph( currentProcess, step, source );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error during mapper configuration", throwable );
      }
    }

  @Override
  public void run( RecordReader input, OutputCollector output, Reporter reporter ) throws IOException
    {
    currentProcess.setReporter( reporter );
    currentProcess.setOutputCollector( output );

    streamGraph.prepare();

    SourceStage head = (SourceStage) streamGraph.getHeads().iterator().next();

    try
      {
      try
        {
        head.run( input );
        }
      catch( IOException exception )
        {
        throw exception;
        }
      catch( Throwable throwable )
        {
        if( throwable instanceof CascadingException )
          throw (CascadingException) throwable;

        throw new FlowException( "internal error during mapper execution", throwable );
        }
      }
    finally
      {
      streamGraph.cleanup();
      }
    }
  }
