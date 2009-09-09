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

package cascading.tap;

import java.io.IOException;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.Scope;
import cascading.scheme.Scheme;
import cascading.scheme.SequenceFile;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/** Class TempHfs creates a temporary {@link Tap} instance for use internally. */
public class TempHfs extends Hfs
  {
  /** Field name */
  final String name;
  /** Field schemeClass */
  private Class schemeClass;
  /** Field temporaryPath */
  private String temporaryPath;

  /** Class NullScheme is a noop scheme used as a placeholder */
  private static class NullScheme extends Scheme
    {

    @Override
    public void sourceInit( Tap tap, Job job ) throws IOException
      {
      // do nothing
      }

    @Override
    public void sinkInit( Tap tap, Job job ) throws IOException
      {
      job.setOutputKeyClass( Tuple.class );
      job.setOutputValueClass( Tuple.class );
      job.setOutputFormatClass( NullOutputFormat.class );
      }

    @Override
    public void source( Tuple tuple, TupleEntryCollector tupleEntryCollector )
      {
      }

    @Override
    public void sink( TupleEntry tupleEntry, TupleEntryCollector tupleEntryCollector ) throws IOException
      {
      }
    }

  /**
   * Constructor TempHfs creates a new TempHfs instance.
   *
   * @param name of type String
   */
  public TempHfs( String name )
    {
    super( new SequenceFile()
    {
    } );
    this.name = name;
    }

  /**
   * Constructor TempHfs creates a new TempHfs instance.
   *
   * @param name   of type String
   * @param isNull of type boolean
   */
  public TempHfs( String name, boolean isNull )
    {
    super( isNull ? new NullScheme() : new SequenceFile()
    {
    } );
    this.name = name;
    }

  /**
   * Constructor TempDfs creates a new TempDfs instance.
   *
   * @param name of type String
   */
  public TempHfs( String name, Class schemeClass )
    {
    this.name = name;

    if( schemeClass == null )
      this.schemeClass = SequenceFile.class;
    else
      this.schemeClass = schemeClass;
    }

  public Class getSchemeClass()
    {
    return schemeClass;
    }

  private void makeTemporaryFile( Configuration conf )
    {
    // init stringPath as path is transient
    if( stringPath != null )
      return;

    temporaryPath = makeTemporaryPathDir( name );
    stringPath = new Path( getTempPath( conf ), temporaryPath ).toString();
    }

  @Override
  public Scope outgoingScopeFor( Set<Scope> incoming )
    {
    // if incoming is Each, both value and group fields are the same
    // if incoming is Every, group fields are only those grouped on
    // if incoming is Group, value fields are all the fields
    Scope scope = incoming.iterator().next();
    Fields outgoingFields = null;

    if( scope.isGroup() )
      outgoingFields = scope.getOutValuesFields();
    else
      outgoingFields = scope.getOutGroupingFields();

    try
      {
      setScheme( (Scheme) schemeClass.getConstructor( Fields.class ).newInstance( outgoingFields ) );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to create specified scheme: " + schemeClass.getName() );
      }

    return new Scope( outgoingFields );
    }

  @Override
  public void sourceInit( Job job ) throws IOException
    {
    makeTemporaryFile( job.getConfiguration() );
    super.sourceInit( job );
    }

  @Override
  public void sinkInit( Job job ) throws IOException
    {
    makeTemporaryFile( job.getConfiguration() );
    super.sinkInit( job );
    }

  @Override
  public boolean deletePath( Job job ) throws IOException
    {
    Configuration conf = job.getConfiguration();
    return super.deletePath( job ) && getFileSystem( conf ).delete( new Path( getTempPath( conf ), temporaryPath ), true );
    }

  @Override
  public String toString()
    {
    return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[" + name + "]";
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

    TempHfs tempHfs = (TempHfs) object;

    if( name != null ? !name.equals( tempHfs.name ) : tempHfs.name != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    // don't use super hashCode() as path changes during runtime
    return 31 * ( System.identityHashCode( this ) + name != null ? name.hashCode() : 0 );
    }
  }
