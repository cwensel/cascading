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
import org.apache.hadoop.mapreduce.Mapper;

/** Class FlowMapper is the Hadoop Mapper implementation. */
public class FlowMapper extends Mapper
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
  protected void setup( Context context ) throws IOException, InterruptedException
    {
    try
      {
      super.setup( context );
      HadoopUtil.initLog4j( context.getConfiguration() );

      currentProcess = new HadoopFlowProcess( new FlowSession(), true );
      flowMapperStack = new FlowMapperStack( currentProcess );

      currentProcess.setContext( context );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error during mapper configuration", throwable );
      }
    }

  @Override
  protected void map( Object key, Object value, Context context ) throws IOException, InterruptedException
    {
    try
      {
      flowMapperStack.map( key, value );
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
  protected void cleanup( Context context ) throws IOException, InterruptedException
    {
    try
      {
      super.cleanup( context );
      }
    finally
      {
      flowMapperStack.close();
      }
    }
  }
