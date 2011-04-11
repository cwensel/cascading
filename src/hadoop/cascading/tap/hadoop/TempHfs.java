/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.hadoop;

import java.io.IOException;
import java.net.URI;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.Scope;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

/** Class TempHfs creates a temporary {@link cascading.tap.Tap} instance for use internally. */
public class TempHfs extends Hfs
  {
  /** Field name */
  final String name;
  /** Field schemeClass */
  private Class schemeClass;
  /** Field temporaryPath */
  private String temporaryPath;

  /** Class NullScheme is a noop scheme used as a placeholder */
  private static class NullScheme extends Scheme<HadoopFlowProcess, JobConf, Object, Object, Object>
    {

    @Override
    public void sourceConfInit( HadoopFlowProcess flowProcess, Tap tap, JobConf conf ) throws IOException
      {
      // do nothing
      }

    @Override
    public void sinkConfInit( HadoopFlowProcess flowProcess, Tap tap, JobConf conf ) throws IOException
      {
      conf.setOutputKeyClass( Tuple.class );
      conf.setOutputValueClass( Tuple.class );
      conf.setOutputFormat( NullOutputFormat.class );
      }

    @Override
    public boolean source( HadoopFlowProcess flowProcess, SourceCall sourceCall ) throws IOException
      {
      return false;
      }

    @Override
    public void sink( HadoopFlowProcess flowProcess, SinkCall sinkCall ) throws IOException
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

  private void makeTemporaryFile( JobConf conf )
    {
    // init stringPath as path is transient
    if( stringPath != null )
      return;

    temporaryPath = makeTemporaryPathDir( name );
    stringPath = new Path( getTempPath( conf ), temporaryPath ).toString();
    }

  @Override
  public URI getURIScheme( JobConf jobConf ) throws IOException
    {
    makeTemporaryFile( jobConf );
    return super.getURIScheme( jobConf );
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
  public void sourceConfInit( HadoopFlowProcess process, JobConf conf ) throws IOException
    {
    makeTemporaryFile( conf );
    super.sourceConfInit( process, conf );
    }

  @Override
  public void sinkConfInit( HadoopFlowProcess process, JobConf conf ) throws IOException
    {
    makeTemporaryFile( conf );
    super.sinkConfInit( process, conf );
    }

  @Override
  public boolean deletePath( JobConf conf ) throws IOException
    {
    if( temporaryPath == null ) // never initialized
      return true;

    return super.deletePath( conf ) && getFileSystem( conf ).delete( new Path( getTempPath( conf ), temporaryPath ), true );
    }

  @Override
  public boolean isTemporary()
    {
    return true;
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
