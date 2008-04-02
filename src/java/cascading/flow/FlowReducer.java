/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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
import cascading.flow.hadoop.HadoopUtil;
import cascading.flow.stack.FlowReducerStack;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/** Class FlowReducer ... */
public class FlowReducer extends MapReduceBase implements Reducer
  {
  private FlowReducerStack flowReducerStack;

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
      flowReducerStack = new FlowReducerStack( jobConf );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error during reducer configuration", throwable );
      }
    }

  public void reduce( WritableComparable key, Iterator values, OutputCollector output, Reporter reporter ) throws IOException
    {
    try
      {
      flowReducerStack.reduce( key, values, output );
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
    super.close();

    flowReducerStack.close();
    }
  }
