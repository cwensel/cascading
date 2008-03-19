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
import cascading.tuple.Tuple;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * A Tap represents the physical data source or sink in a connected {@link Flow}.
 * That is...a source Tap is the head end of a connected {@link Pipe} and {@link Tuple} stream, and
 * a sink Tap is the tail end. Kinds of Tap types are used to manage files from a local disk,
 * distributed disk, remote storage like Amazon S3, or via FTP. It simply abstracts
 * out the complexity of connecting to these types of data sources.
 * <p/>
 * A Tap takes a {@link Scheme} instance, which is used to identify the type of resource. A Tap is responsible for
 * how the resource is reached.
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
  Scheme scheme;

  /** Field writeDirect */
  boolean useTapCollector = false;

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
   * Method isUseTapCollector returns true if this instances {@link TapCollector} should be used to sink values.
   *
   * @return the writeDirect (type boolean) of this Tap object.
   */
  public boolean isUseTapCollector()
    {
    return useTapCollector;
    }

  /**
   * Method setUseTapCollector should be set to true if this instances {@link TapCollector} should be used to sink values.
   *
   * @param useTapCollector the writeDirect of this Tap object.
   */
  public void setUseTapCollector( boolean useTapCollector )
    {
    this.useTapCollector = useTapCollector;
    }

  /**
   * Method sourceInit initializes this instance as a source.
   *
   * @param conf of type JobConf
   * @throws IOException on resource initialization failure.
   */
  public void sourceInit( JobConf conf ) throws IOException
    {
    scheme.sourceInit( this, conf );
    }

  /**
   * Method sinkInit initializes this instance as a sink.
   *
   * @param conf of type JobConf
   * @throws IOException on resource initialization failure.
   */
  public void sinkInit( JobConf conf ) throws IOException
    {
    scheme.sinkInit( this, conf );
    }

  /**
   * Method getPath returns the Hadoop path to the resource represented by this Tap instance.
   *
   * @return Path
   */
  public abstract Path getPath();

  /**
   * Method containsFile indicates whether the tap contains a given file.
   *
   * @param conf        of type JobConf
   * @param currentFile of type String
   * @return boolean
   */
  public abstract boolean containsFile( JobConf conf, String currentFile );

  /**
   * Method getSourceFields returns the sourceFields of this Tap object.
   *
   * @return the sourceFields (type Fields) of this Tap object.
   */
  public Fields getSourceFields()
    {
    return scheme.getSourceFields();
    }

  /**
   * Method getSinkFields returns the sinkFields of this Tap object.
   *
   * @return the sinkFields (type Fields) of this Tap object.
   */
  public Fields getSinkFields()
    {
    return scheme.getSinkFields();
    }

  /**
   * Method openForRead opens the resource represented by this Tap instance.
   *
   * @param conf of type JobConf
   * @return TapIterator
   * @throws IOException when the resource cannot be opened
   */
  public TapIterator openForRead( JobConf conf ) throws IOException
    {
    return new TapIterator( this, conf );
    }

  /**
   * Method openForWrite opens the resource represented by this Tap instance.
   *
   * @param conf of type JobConf
   * @return TapCollector
   * @throws IOException when
   */
  public TapCollector openForWrite( JobConf conf ) throws IOException
    {
    return new TapCollector( this, conf );
    }

  /**
   * Method source returns the source value as an instance of {@link Tuple}
   *
   * @param key   of type WritableComparable
   * @param value of type Writable
   * @return Tuple
   */
  public Tuple source( WritableComparable key, Writable value )
    {
    return scheme.source( key, value );
    }

  /**
   * Method sink emits the sink value(s) to the OutputCollector
   *
   * @param fields          of type Fields
   * @param tuple           of type Tuple
   * @param outputCollector of type OutputCollector
   * @throws IOException when the resource cannot be written to
   */
  public void sink( Fields fields, Tuple tuple, OutputCollector outputCollector ) throws IOException
    {
    scheme.sink( fields, tuple, outputCollector );
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
        incomingFields.verifyContains( getSinkFields() );
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
   * Method isDeleteOnSinkInit indicates whether the resource represented by this instance should be deleted if it
   * already exists when the tap is initialized.
   *
   * @return boolean
   */
  public boolean isDeleteOnSinkInit()
    {
    return false;
    }

  /**
   * Method isSink returns true if this Tap instance can be used as a sink.
   *
   * @return the sink (type boolean) of this Tap object.
   */
  public boolean isSink()
    {
    return true;
    }

  /**
   * Method isSource returns true if this Tap instance can be used as a source.
   *
   * @return the source (type boolean) of this Tap object.
   */
  public boolean isSource()
    {
    return true;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Tap tap = (Tap) object;

    if( scheme != null ? !scheme.equals( tap.scheme ) : tap.scheme != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return scheme != null ? scheme.hashCode() : 0;
    }

  }
