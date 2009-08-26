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

package cascading.flow.hadoop;

import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.io.IOException;

/**
 * Class HadoopFlowProcess is an implemenation of {@link FlowProcess} for Hadoop. Use this interfact to get direct
 * access to the Hadoop JobConf and Reporter interfaces.
 * <p/>
 * Be warned that coupling to this implemenation will cause custom {@link cascading.operation.Operation}s to
 * fail if they are executed on a system other than Hadoop.
 *
 * @see cascading.flow.FlowSession
 * @see JobConf
 * @see Reporter
 */
public class HadoopFlowProcess extends FlowProcess
  {
  /** Field isMapper */
  private boolean isMapper;
  /** Field reporter */
  TaskInputOutputContext context;

  /**
   * Constructor HadoopFlowProcess creates a new HadoopFlowProcess instance.
   *
   * @param flowSession of type FlowSession
   */
  public HadoopFlowProcess( FlowSession flowSession, boolean isMapper )
    {
    super( flowSession );
    this.isMapper = isMapper;
    }

  /**
   * Method getJobConf returns the jobConf of this HadoopFlowProcess object.
   *
   * @return the jobConf (type JobConf) of this HadoopFlowProcess object.
   */
  public Configuration getConfiguration()
    {
    return context.getConfiguration();
    }

  /**
   * Method isMapper returns true if this part of the FlowProcess is a MapReduce mapper. If false, it is a reducer.
   *
   * @return boolean
   */
  public boolean isMapper()
    {
    return isMapper;
    }

  public int getCurrentNumMappers()
    {
    return getConfiguration().getInt( "mapred.map.tasks", 1 );
    }

  public int getCurrentNumReducers()
    {
    return getConfiguration().getInt( "mapred.reduce.tasks", 1 );
    }

  /**
   * Method getCurrentTaskNum returns the task number of this task. Task 0 is the first task.
   *
   * @return int
   */
  public int getCurrentTaskNum()
    {
    return getConfiguration().getInt( "mapred.task.partition", 0 );
    }

  /**
   * Method setReporter sets the reporter of this HadoopFlowProcess object.
   *
   * @param context the reporter of this HadoopFlowProcess object.
   */
  public void setContext( TaskInputOutputContext context )
    {
    this.context = context;
    }

  /**
   * Method getReporter returns the reporter of this HadoopFlowProcess object.
   *
   * @return the reporter (type Reporter) of this HadoopFlowProcess object.
   */
  public TaskInputOutputContext getContext()
    {
    return context;
    }

  /** @see cascading.flow.FlowProcess#getProperty(String) */
  public Object getProperty( String key )
    {
    return getConfiguration().get( key );
    }

  /** @see cascading.flow.FlowProcess#keepAlive() */
  public void keepAlive()
    {
    context.progress();
    }

  /** @see cascading.flow.FlowProcess#increment(Enum, int) */
  public void increment( Enum counter, int amount )
    {
    context.getCounter( counter ).increment( amount );
    }

  /** @see cascading.flow.FlowProcess#setStatus(String) */
  public void setStatus( String status )
    {
    context.setStatus( status );
    }

  /** @see cascading.flow.FlowProcess#openTapForRead(Tap) */
  public TupleEntryIterator openTapForRead( Tap tap ) throws IOException
    {
    return tap.openForRead( getConfiguration() );
    }

  /** @see cascading.flow.FlowProcess#openTapForWrite(Tap) */
  public TupleEntryCollector openTapForWrite( Tap tap ) throws IOException
    {
    return tap.openForWrite( getConfiguration() );
    }
  }
