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
import cascading.tuple.Tuple;
import cascading.tuple.TupleIterator;
import org.apache.hadoop.mapreduce.Mapper;

/** Class FlowMapper is the Hadoop Mapper implementation. */
public class FlowMapper extends Mapper
  {
  /** Field flowMapperStack */
  private FlowMapperStack flowMapperStack;
  /** Field currentProcess */
  private HadoopFlowProcess currentProcess;

  private class ContextIterator implements TupleIterator
    {
    Context context = null;
    Tuple pair = new Tuple( null, null );

    private ContextIterator( Context context )
      {
      this.context = context;
      }

    @Override
    public boolean hasNext()
      {
      try
        {
        return context.nextKeyValue();
        }
      catch( IOException exception )
        {
        throw new RuntimeException( exception );
        }
      catch( InterruptedException exception )
        {
        throw new RuntimeException( exception );
        }
      }

    @Override
    public Tuple next()
      {
      currentProcess.increment( StepCounters.Tuples_Read, 1 );

      try
        {
        pair = Tuple.asModifiable( pair );

        pair.set( 0, context.getCurrentKey() );
        pair.set( 1, context.getCurrentValue() );

        pair = Tuple.asUnmodifiable( pair );

        return pair;
        }
      catch( IOException exception )
        {
        throw new RuntimeException( exception );
        }
      catch( InterruptedException exception )
        {
        throw new RuntimeException( exception );
        }
      }

    @Override
    public void remove()
      {
      throw new UnsupportedOperationException( "remove is not supported" );
      }

    @Override
    public void close()
      {
      // do nothing
      }
    }

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

      currentProcess = new HadoopFlowProcess( new FlowSession(), context, true );
      flowMapperStack = new FlowMapperStack( currentProcess );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error during mapper configuration", throwable );
      }
    }

  @Override
  public void run( Context context ) throws IOException, InterruptedException
    {
    setup( context );

    try
      {
      flowMapperStack.map( new ContextIterator( context ) );
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
    finally
      {
      cleanup( context );
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
