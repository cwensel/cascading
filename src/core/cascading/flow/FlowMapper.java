/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow;

import java.io.IOException;

import cascading.CascadingException;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.HadoopUtil;
import cascading.flow.stack.FlowMapperStack;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/** Class FlowMapper is the Hadoop Mapper implementation. */
public class FlowMapper extends MapReduceBase implements Mapper
  {
  /** Field flowMapperStack */
  private FlowMapperStack flowMapperStack;
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
      super.configure( jobConf );
      HadoopUtil.initLog4j( jobConf );

      currentProcess = new HadoopFlowProcess( new FlowSession(), jobConf, true );
      flowMapperStack = new FlowMapperStack( currentProcess );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error during mapper configuration", throwable );
      }
    }

  public void map( Object key, Object value, OutputCollector output, Reporter reporter ) throws IOException
    {
    currentProcess.setReporter( reporter );

    try
      {
      flowMapperStack.map( key, value, output );
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

  @Override
  public void close() throws IOException
    {
    try
      {
      super.close();
      }
    finally
      {
      flowMapperStack.close();
      }
    }
  }
