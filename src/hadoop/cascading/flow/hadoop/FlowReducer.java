/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.flow.hadoop;

import java.io.IOException;
import java.util.Iterator;

import cascading.CascadingException;
import cascading.flow.FlowException;
import cascading.flow.FlowSession;
import cascading.tuple.Tuple;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/** Class FlowReducer is the Hadoop Reducer implementation. */
public class FlowReducer extends MapReduceBase implements Reducer
  {
  /** Field flowReducerStack */
  private HadoopReduceStreamGraph streamGraph;
  /** Field currentProcess */
  private HadoopFlowProcess currentProcess;

  private boolean calledPrepare = false;
  private HadoopGroupGate group;

  /** Constructor FlowReducer creates a new FlowReducer instance. */
  public FlowReducer()
    {
    }

  @Override
  public void configure( JobConf jobConf )
    {
    try
      {
      super.configure( jobConf );
      HadoopUtil.initLog4j( jobConf );
      currentProcess = new HadoopFlowProcess( new FlowSession(), jobConf, false );

      HadoopFlowStep step = (HadoopFlowStep) HadoopUtil.deserializeBase64( jobConf.getRaw( "cascading.flow.step" ) );

      streamGraph = new HadoopReduceStreamGraph( currentProcess, step );

      group = (HadoopGroupGate) streamGraph.getHeads().iterator().next();
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error during reducer configuration", throwable );
      }
    }

  public void reduce( Object key, Iterator values, OutputCollector output, Reporter reporter ) throws IOException
    {
    currentProcess.setReporter( reporter );
    currentProcess.setOutputCollector( output );

    if( !calledPrepare )
      {
      streamGraph.prepare();

      calledPrepare = true;
      }

    try
      {
      group.run( (Tuple) key, values );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error during reducer execution", throwable );
      }
    }

  @Override
  public void close() throws IOException
    {
    streamGraph.cleanup();

    super.close();
    }
  }
