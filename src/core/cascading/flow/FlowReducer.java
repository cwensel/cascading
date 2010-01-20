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
import java.util.Iterator;

import cascading.CascadingException;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.hadoop.HadoopUtil;
import cascading.flow.stack.FlowReducerStack;
import cascading.tuple.Tuple;
import cascading.tuple.TupleIterator;
import org.apache.hadoop.mapreduce.Reducer;

/** Class FlowReducer is the Hadoop Reducer implementation. */
public class FlowReducer extends Reducer
  {
  /** Field flowReducerStack */
  private FlowReducerStack flowReducerStack;
  /** Field currentProcess */
  private HadoopFlowProcess currentProcess;

  public class ContextGroupIterator implements TupleIterator
    {
    Context context;
    private Tuple groupingTuple = new Tuple(); // a hack to prevent a null check below

    private ContextGroupIterator( Context context )
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
//      currentProcess.increment( StepCounters.Tuples_Read, 1 );

      groupingTuple = Tuple.asUnmodifiable( groupingTuple );
      try
        {
        groupingTuple = (Tuple) context.getCurrentKey();
        }
      catch( IOException exception )
        {
        throw new CascadingException( exception );
        }
      catch( InterruptedException exception )
        {
        throw new CascadingException( exception );
        }

      groupingTuple = Tuple.asUnmodifiable( groupingTuple );

      return groupingTuple;
      }

    public Iterator<Tuple> nextValues()
      {
      try
        {
        return (Iterator<Tuple>) context.getValues().iterator();
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

  /** Constructor FlowReducer creates a new FlowReducer instance. */
  public FlowReducer()
    {
    }

  @Override
  protected void setup( Context context ) throws IOException, InterruptedException
    {
    try
      {
      super.setup( context );
      HadoopUtil.initLog4j( context.getConfiguration() );
      currentProcess = new HadoopFlowProcess( new FlowSession(), context, false );
      flowReducerStack = new FlowReducerStack( currentProcess );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error during reducer configuration", throwable );
      }
    }

  @Override
  public void run( Context context ) throws IOException, InterruptedException
    {
    setup( context );

    try
      {
      flowReducerStack.reduce( new ContextGroupIterator( context ) );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error during reducer execution", throwable );
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
      super.cleanup( context );    //To change body of overridden methods use File | Settings | File Templates.
      }
    finally
      {
      flowReducerStack.close();
      }
    }
  }
