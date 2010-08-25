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

package cascading.tap;

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.Scope;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.FieldsResolverException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * A Tap represents the physical data source or sink in a connected {@link Flow}.
 * </p>
 * That is, a source Tap is the head end of a connected {@link Pipe} and {@link Tuple} stream, and
 * a sink Tap is the tail end. Kinds of Tap types are used to manage files from a local disk,
 * distributed disk, remote storage like Amazon S3, or via FTP. It simply abstracts
 * out the complexity of connecting to these types of data sources.
 * <p/>
 * A Tap takes a {@link Scheme} instance, which is used to identify the type of resource (text file, binary file, etc).
 * A Tap is responsible for how the resource is reached.
 * <p/>
 * A Tap is not given an explicit name by design. This is so a given Tap instance can be
 * re-used in different {@link Flow}s that may expect a source or sink by a different
 * logical name, but are the same physical resource. If a tap had a name other than its path, which would be
 * used for the tap identity? If the name, then two Tap instances with different names but the same path could
 * interfere with one another.
 */
public abstract class Tap implements FlowElement, Serializable
  {
  /** Field scheme */
  private Scheme scheme;

  /** Field writeDirect */
  boolean writeDirect = false;
  /** Field mode */
  SinkMode sinkMode = SinkMode.KEEP;

  /**
   * Convenience function to make an array of Tap instances.
   *
   * @param taps of type Tap
   * @return Tap array
   */
  public static Tap[] taps( Tap... taps )
    {
    return taps;
    }

  protected Tap()
    {
    }

  protected Tap( Scheme scheme )
    {
    this.setScheme( scheme );
    }

  protected Tap( Scheme scheme, SinkMode sinkMode )
    {
    this.setScheme( scheme );
    this.sinkMode = sinkMode;
    }

  protected void setScheme( Scheme scheme )
    {
    this.scheme = scheme;
    }

  /**
   * Method getScheme returns the scheme of this Tap object.
   *
   * @return the scheme (type Scheme) of this Tap object.
   */
  public Scheme getScheme()
    {
    return scheme;
    }

  /**
   * Method isWriteDirect returns true if this instances {@link cascading.tuple.TupleEntryCollector} should be used to sink values.
   *
   * @return the writeDirect (type boolean) of this Tap object.
   */
  public boolean isWriteDirect()
    {
    return writeDirect || getScheme().isWriteDirect();
    }

  /**
   * Method setWriteDirect should be set to true if this instances {@link cascading.tuple.TupleEntryCollector} should be used to sink values.
   *
   * @param writeDirect the writeDirect of this Tap object.
   */
  public void setWriteDirect( boolean writeDirect )
    {
    this.writeDirect = writeDirect;
    }

  /**
   * Method flowInit allows this Tap instance to initalize itself in context of the given {@link Flow} instance.
   * This method is guaranteed to be called before the Flow is started and the
   * {@link cascading.flow.FlowListener#onStarting(cascading.flow.Flow)} event is fired.
   * <p/>
   * This method will be called once per Flow, and before {@link #sourceInit(org.apache.hadoop.mapred.JobConf)} and
   * {@link #sinkInit(org.apache.hadoop.mapred.JobConf)} methods.
   *
   * @param flow of type Flow
   */
  public void flowInit( Flow flow )
    {

    }

  /**
   * Method sourceInit initializes this instance as a source.
   * <p/>
   * This method maybe called more than once if this Tap instance is used outside the scope of a {@link Flow}
   * instance or if it participates in multiple times in a given Flow or across different Flows in
   * a {@link cascading.cascade.Cascade}.
   * <p/>
   * In the context of a Flow, it will be called after
   * {@link cascading.flow.FlowListener#onStarting(cascading.flow.Flow)}
   *
   * @param conf of type JobConf
   * @throws IOException on resource initialization failure.
   */
  public void sourceInit( JobConf conf ) throws IOException
    {
    getScheme().sourceInit( this, conf );
    }

  /**
   * Method sinkInit initializes this instance as a sink.
   * <p/>
   * This method maybe called more than once if this Tap instance is used outside the scope of a {@link Flow}
   * instance or if it participates in multiple times in a given Flow or across different Flows in
   * a {@link cascading.cascade.Cascade}.
   * <p/>
   * Note this method will be called in context of this Tap being used as a traditional 'sink' and as a 'trap'.
   * <p/>
   * In the context of a Flow, it will be called after
   * {@link cascading.flow.FlowListener#onStarting(cascading.flow.Flow)}
   *
   * @param conf of type JobConf
   * @throws IOException on resource initialization failure.
   */
  public void sinkInit( JobConf conf ) throws IOException
    {
    getScheme().sinkInit( this, conf );
    }

  /**
   * Method getPath returns the Hadoop path to the resource represented by this Tap instance.
   *
   * @return Path
   */
  public abstract Path getPath();

  /**
   * Method getIdentifier returns a String representing the resource identifier this Tap instance represents.
   * <p/>
   * By default, simply calls {@code getPath().toString()}.
   *
   * @return the resource (type String) of this Tap object.
   */
  public String getIdentifier()
    {
    if( getPath() == null )
      return null;

    return getPath().toString();
    }

  /**
   * Method getSourceFields returns the sourceFields of this Tap object.
   *
   * @return the sourceFields (type Fields) of this Tap object.
   */
  public Fields getSourceFields()
    {
    return getScheme().getSourceFields();
    }

  /**
   * Method getSinkFields returns the sinkFields of this Tap object.
   *
   * @return the sinkFields (type Fields) of this Tap object.
   */
  public Fields getSinkFields()
    {
    return getScheme().getSinkFields();
    }

  /**
   * Method openForRead opens the resource represented by this Tap instance.
   *
   * @param conf of type JobConf
   * @return TupleEntryIterator
   * @throws java.io.IOException when the resource cannot be opened
   */
  public abstract TupleEntryIterator openForRead( JobConf conf ) throws IOException;

  /**
   * Method openForWrite opens the resource represented by this Tap instance.
   *
   * @param conf of type JobConf
   * @return TupleEntryCollector
   * @throws java.io.IOException when
   */
  public abstract TupleEntryCollector openForWrite( JobConf conf ) throws IOException;

  /**
   * Method source returns the source value as an instance of {@link Tuple}
   *
   * @param key   of type WritableComparable
   * @param value of type Writable
   * @return Tuple
   */
  public Tuple source( Object key, Object value )
    {
    return getScheme().source( key, value );
    }

  /**
   * Method sink emits the sink value(s) to the OutputCollector
   *
   * @param tupleEntry      of type TupleEntry
   * @param outputCollector of type OutputCollector
   * @throws java.io.IOException when the resource cannot be written to
   */
  public void sink( TupleEntry tupleEntry, OutputCollector outputCollector ) throws IOException
    {
    getScheme().sink( tupleEntry, outputCollector );
    }

  /** @see FlowElement#outgoingScopeFor(Set<Scope>) */
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    // as a source Tap, we emit the scheme defined Fields
    // as a sink Tap, we declare we emit the incoming Fields
    // as a temp Tap, this method never gets called, but we emit what we consume
    int count = 0;
    for( Scope incomingScope : incomingScopes )
      {
      Fields incomingFields = resolveFields( incomingScope );

      if( incomingFields != null )
        {
        try
          {
          incomingFields.select( getSinkFields() );
          }
        catch( FieldsResolverException exception )
          {
          throw new TapException( this, exception.getSourceFields(), exception.getSelectorFields(), exception );
          }

        count++;
        }
      }

    if( count > 1 )
      throw new FlowException( "Tap may not have more than one incoming Scope" );

    if( count == 1 )
      return new Scope( getSinkFields() );

    return new Scope( getSourceFields() );
    }

  /** @see FlowElement#resolveIncomingOperationFields(Scope) */
  public Fields resolveIncomingOperationFields( Scope incomingScope )
    {
    return getFieldsFor( incomingScope );
    }

  /** @see FlowElement#resolveFields(Scope) */
  public Fields resolveFields( Scope scope )
    {
    return getFieldsFor( scope );
    }

  private Fields getFieldsFor( Scope incomingScope )
    {
    if( incomingScope.isEvery() )
      return incomingScope.getOutGroupingFields();
    else
      return incomingScope.getOutValuesFields();
    }

  /**
   * Method getQualifiedPath returns a FileSystem fully qualified Hadoop Path.
   *
   * @param conf of type JobConf
   * @return Path
   * @throws IOException when
   */
  public Path getQualifiedPath( JobConf conf ) throws IOException
    {
    return getPath();
    }

  /**
   * Method makeDirs makes all the directories this Tap instance represents.
   *
   * @param conf of type JobConf
   * @return boolean
   * @throws IOException when there is an error making directories
   */
  public abstract boolean makeDirs( JobConf conf ) throws IOException;

  /**
   * Method deletePath deletes the resource represented by this instance.
   *
   * @param conf of type JobConf
   * @return boolean
   * @throws IOException when the resource cannot be deleted
   */
  public abstract boolean deletePath( JobConf conf ) throws IOException;

  /**
   * Method pathExists return true if the path represented by this instance exists.
   *
   * @param conf of type JobConf
   * @return boolean
   * @throws IOException when the status cannot be determined
   */
  public abstract boolean pathExists( JobConf conf ) throws IOException;

  /**
   * Method getPathModified returns the date this resource was last modified.
   *
   * @param conf of type JobConf
   * @return long
   * @throws IOException when the modified date cannot be determined
   */
  public abstract long getPathModified( JobConf conf ) throws IOException;

  /**
   * Method isKeep indicates whether the resource represented by this instance should be kept if it
   * already exists when the Flow is started.
   *
   * @return boolean
   */
  public boolean isKeep()
    {
    return sinkMode == SinkMode.KEEP;
    }

  /**
   * Method isReplace indicates whether the resource represented by this instance should be deleted if it
   * already exists when the Flow is started.
   *
   * @return boolean
   */
  public boolean isReplace()
    {
    return sinkMode == SinkMode.REPLACE;
    }

  /**
   * Method isAppend indicates whether the resrouce represented by this instance should be appended to if it already
   * exists. Otherwise a new resource will be created when the Flow is started..
   *
   * @return boolean
   */
  @Deprecated
  public boolean isAppend()
    {
    return sinkMode == SinkMode.APPEND;
    }

  /**
   * Method isUpdate indicates whether the resrouce represented by this instance should be updated if it already
   * exists. Otherwise a new resource will be created when the Flow is started..
   *
   * @return boolean
   */
  public boolean isUpdate()
    {
    return isAppend() || sinkMode == SinkMode.UPDATE;
    }

  /**
   * Method isSink returns true if this Tap instance can be used as a sink.
   *
   * @return boolean
   */
  public boolean isSink()
    {
    return getScheme().isSink();
    }

  /**
   * Method isSource returns true if this Tap instance can be used as a source.
   *
   * @return boolean
   */
  public boolean isSource()
    {
    return getScheme().isSource();
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Tap tap = (Tap) object;

    if( getScheme() != null ? !getScheme().equals( tap.getScheme() ) : tap.getScheme() != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return getScheme() != null ? getScheme().hashCode() : 0;
    }

  }
