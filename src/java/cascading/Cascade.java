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

package cascading;

import java.util.HashSet;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowException;
import cascading.tap.Tap;
import org.apache.log4j.Logger;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;

/**
 * A Cascade is an assembly of {@link Flow} instances that share or depend the same {@link Tap} instances and are executed as
 * a single group. The most common case is where one Flow instance depends on a Tap created by a second Flow instance. This
 * dependency chain can continue as practical.
 * <p/>
 * Additionally, a Cascade allows for incremental builds of complex data processing processes. If a given source {@link Tap} is newer than
 * a subsequent sink {@link Tap} in the assembly, the connecting {@link Flow}(s) will be executed
 * when the Cascade executed. If all the targets (sinks) are up to date, the Cascade exits immediately and does nothing.
 */
public class Cascade implements Runnable
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( Cascade.class );

  /** Field name */
  private String name;
  /** Field rootTap */
  private final Tap rootTap;
  /** Field graph */
  private final SimpleDirectedGraph<Tap, Flow.FlowHolder> graph;
  /** Field thread */
  private Thread thread;
  /** Field throwable */
  private Throwable throwable;

  /**
   * Constructor Cascade creates a new Cascade instance.
   *
   * @param name    of type String
   * @param rootTap of type Tap
   * @param graph   of type SimpleDirectedGraph<Tap, FlowHolder>
   */
  Cascade( String name, Tap rootTap, SimpleDirectedGraph<Tap, Flow.FlowHolder> graph )
    {
    this.name = name;
    this.rootTap = rootTap;
    this.graph = graph;
    }

  /**
   * Method getName returns the name of this Cascade object.
   *
   * @return the name (type String) of this Cascade object.
   */
  public String getName()
    {
    return name;
    }

  /**
   * Method start begins the current Cascade process. It returns immediately. See method {@link #complete()} to block
   * until the Cascade completes.
   */
  public void start()
    {
    if( thread != null )
      return;

    // todo: thread pooling might be smart
    thread = new Thread( this, "flow" );

    thread.start();
    }

  /**
   * Method complete begins the current Cascade process if method {@link #start()} was not previously called. This method
   * blocks until the process completes.
   *
   * @throws RuntimeException wrapping any exception thrown internally.
   */
  public void complete()
    {
    start();

    try
      {
      try
        {
        thread.join();
        }
      catch( InterruptedException exception )
        {
        throw new FlowException( "thread interrupted", exception );
        }

      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      if( throwable != null )
        throw new CascadeException( "unhandled exception", throwable );
      }
    finally
      {
      thread = null;
      throwable = null;
      }
    }

  /** Method run implements the Runnable run method. */
  public void run()
    {
    if( LOG.isInfoEnabled() )
      LOG.info( "starting cascade: " + getName() );

    // todo: test for writing to same path
    try
      {
      Set<Flow> completed = new HashSet<Flow>();
      TopologicalOrderIterator<Tap, Flow.FlowHolder> iterator = new TopologicalOrderIterator<Tap, Flow.FlowHolder>( graph );

      while( iterator.hasNext() )
        {
        Tap source = iterator.next();

        if( source instanceof CascadeConnector.RootTap )
          continue;

        if( LOG.isDebugEnabled() )
          LOG.debug( "evaluating source: " + source );

        Set<Flow.FlowHolder> flows = graph.outgoingEdgesOf( source );
        Set<Flow> startable = new HashSet<Flow>();

        for( Flow.FlowHolder flowHolder : flows )
          {
          if( completed.contains( flowHolder.flow ) )
            continue;

          if( flowHolder.flow.areSinksStale() )
            startable.add( flowHolder.flow );
          else if( LOG.isInfoEnabled() )
            LOG.info( "skipping flow: " + flowHolder.flow.getName() );
          }

        if( LOG.isDebugEnabled() )
          LOG.debug( "starting flows: " + startable.size() );

        for( Flow flow : startable )
          {
          if( LOG.isInfoEnabled() )
            LOG.info( "starting flow: " + flow.getName() );

          flow.deleteSinks();
          flow.start();
          }

        for( Flow flow : startable )
          {
          flow.complete();

          if( LOG.isInfoEnabled() )
            LOG.info( "completed flow: " + flow.getName() );
          }

        completed.addAll( startable );
        }
      }
    catch( Throwable throwable )
      {
      this.throwable = throwable;
      }
    }

  @Override
  public String toString()
    {
    return getName();
    }
  }
