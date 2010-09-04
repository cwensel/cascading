/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.hadoop.HadoopUtil;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;
import riffle.process.scheduler.ProcessException;
import riffle.process.scheduler.ProcessWrapper;

/**
 * Class ProcessFlow is a {@link cascading.flow.Flow} subclass that supports custom Riffle jobs.
 * <p/>
 * Use this class to allow custom Riffle jobs to participate in the {@link cascading.cascade.Cascade} scheduler. If
 * other Flow instances in the Cascade share resources with this Flow instance, all participants will be scheduled
 * according to their dependencies (topologically).
 */
public class ProcessFlow<P> extends Flow
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( ProcessFlow.class );

  /** Field process */
  private P process;
  /** Field processWrapper */
  private ProcessWrapper processWrapper;

  /**
   * Constructor ProcessFlow creates a new ProcessFlow instance.
   *
   * @param name    of type String
   * @param process of type JobConf
   */
  @ConstructorProperties({"name", "process"})
  public ProcessFlow( String name, P process )
    {
    this( null, name, process );
    }

  /**
   * Constructor ProcessFlow creates a new ProcessFlow instance.
   *
   * @param properties of type Map<Object, Object>
   * @param name       of type String
   * @param process    of type P
   */
  @ConstructorProperties({"properties", "name", "process"})
  public ProcessFlow( Map<Object, Object> properties, String name, P process )
    {
    super( properties, getJobConf( properties ), name );
    this.process = process;
    this.processWrapper = new ProcessWrapper( this.process );

    setName( name );
    setTapFromProcess();
    }

  private static JobConf getJobConf( Map<Object, Object> properties )
    {
    return HadoopUtil.createJobConf( properties, MultiMapReducePlanner.getJobConf( properties ) );
    }

  /**
   * Method setTapFromProcess build {@link Tap} instance for the give process incoming and outgoing dependencies.
   * <p/>
   * This method may be called repeatedly to re-configure the source and sink taps.
   */
  public void setTapFromProcess()
    {
    setSources( createSources( this.processWrapper ) );
    setSinks( createSinks( this.processWrapper ) );
    setTraps( createTraps( this.processWrapper ) );
    }

  /**
   * Method getProcess returns the process of this ProcessFlow object.
   *
   * @return the process (type P) of this ProcessFlow object.
   */
  public P getProcess()
    {
    return process;
    }

  @Override
  public void prepare()
    {
    try
      {
      processWrapper.prepare();
      }
    catch( ProcessException exception )
      {
      if( exception.getCause() instanceof RuntimeException )
        throw (RuntimeException) exception.getCause();

      throw new FlowException( "could not call prepare on process", exception.getCause() );
      }
    }

  @Override
  public void start()
    {
    try
      {
      processWrapper.start();
      }
    catch( ProcessException exception )
      {
      if( exception.getCause() instanceof RuntimeException )
        throw (RuntimeException) exception.getCause();

      throw new FlowException( "could not call start on process", exception.getCause() );
      }
    }

  @Override
  public void stop()
    {
    try
      {
      processWrapper.stop();
      }
    catch( ProcessException exception )
      {
      if( exception.getCause() instanceof RuntimeException )
        throw (RuntimeException) exception.getCause();

      throw new FlowException( "could not call stop on process", exception.getCause() );
      }
    }

  @Override
  public void complete()
    {
    try
      {
      processWrapper.complete();
      }
    catch( ProcessException exception )
      {
      if( exception.getCause() instanceof RuntimeException )
        throw (RuntimeException) exception.getCause();

      throw new FlowException( "could not call complete on process", exception.getCause() );
      }
    }

  @Override
  public void cleanup()
    {
    try
      {
      processWrapper.cleanup();
      }
    catch( ProcessException exception )
      {
      if( exception.getCause() instanceof RuntimeException )
        throw (RuntimeException) exception.getCause();

      throw new FlowException( "could not call cleanup on process", exception.getCause() );
      }
    }

  private Map<String, Tap> createSources( ProcessWrapper processParent )
    {
    try
      {
      return makeTapMap( processParent.getDependencyIncoming() );
      }
    catch( ProcessException exception )
      {
      if( exception.getCause() instanceof RuntimeException )
        throw (RuntimeException) exception.getCause();

      throw new FlowException( "could not get process incoming dependency", exception.getCause() );
      }
    }

  private Map<String, Tap> createSinks( ProcessWrapper processParent )
    {
    try
      {
      return makeTapMap( processParent.getDependencyOutgoing() );
      }
    catch( ProcessException exception )
      {
      if( exception.getCause() instanceof RuntimeException )
        throw (RuntimeException) exception.getCause();

      throw new FlowException( "could not get process outgoing dependency", exception.getCause() );
      }
    }

  private Map<String, Tap> makeTapMap( Object resource )
    {
    Collection paths = makeCollection( resource );

    Map<String, Tap> taps = new HashMap<String, Tap>();

    for( Object path : paths )
      {
      if( path instanceof Tap )
        taps.put( ( (Tap) path ).getIdentifier(), (Tap) path );
      else
        taps.put( path.toString(), new ProcessTap( new NullScheme(), path.toString() ) );
      }
    return taps;
    }

  private Collection makeCollection( Object resource )
    {
    if( resource instanceof Collection )
      return (Collection) resource;
    else if( resource instanceof Object[] )
      return Arrays.asList( (Object[]) resource );
    else
      return Arrays.asList( resource );
    }

  private Map<String, Tap> createTraps( ProcessWrapper processParent )
    {
    return new HashMap<String, Tap>();
    }

  @Override
  public String toString()
    {
    return getName() + ":" + process;
    }

  static class NullScheme extends Scheme
    {
    public void sourceInit( Tap tap, JobConf conf ) throws IOException
      {
      }

    public void sinkInit( Tap tap, JobConf conf ) throws IOException
      {
      }

    public Tuple source( Object key, Object value )
      {
      if( value instanceof Comparable )
        return new Tuple( (Comparable) key, (Comparable) value );
      else
        return new Tuple( (Comparable) key );
      }

    @Override
    public String toString()
      {
      return getClass().getSimpleName();
      }

    public void sink( TupleEntry tupleEntry, OutputCollector outputCollector ) throws IOException
      {
      throw new UnsupportedOperationException( "sinking is not supported in the scheme" );
      }
    }

  /**
   *
   */
  static class ProcessTap extends Tap
    {
    private String token;

    ProcessTap( NullScheme scheme, String token )
      {
      super( scheme );
      this.token = token;
      }

    @Override
    public Path getPath()
      {
      return new Path( token );
      }

    @Override
    public TupleEntryIterator openForRead( JobConf conf ) throws IOException
      {
      return null;
      }

    @Override
    public TupleEntryCollector openForWrite( JobConf conf ) throws IOException
      {
      return null;
      }

    @Override
    public boolean makeDirs( JobConf conf ) throws IOException
      {
      return false;
      }

    @Override
    public boolean deletePath( JobConf conf ) throws IOException
      {
      return false;
      }

    @Override
    public boolean pathExists( JobConf conf ) throws IOException
      {
      return false;
      }

    @Override
    public long getPathModified( JobConf conf ) throws IOException
      {
      return 0;
      }

    @Override
    public String toString()
      {
      return token;
      }

    @Override
    public boolean equals( Object object )
      {
      if( this == object )
        return true;
      if( object == null || getClass() != object.getClass() )
        return false;
      if( !super.equals( object ) )
        return false;

      ProcessTap that = (ProcessTap) object;

      if( token != null ? !token.equals( that.token ) : that.token != null )
        return false;

      return true;
      }

    @Override
    public int hashCode()
      {
      int result = super.hashCode();
      result = 31 * result + ( token != null ? token.hashCode() : 0 );
      return result;
      }
    }
  }