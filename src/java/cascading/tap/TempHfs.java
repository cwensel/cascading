/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.Scope;
import cascading.scheme.SequenceFile;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/** Class TempHfs creates a temporary {@link Tap} instance for use internally. */
public class TempHfs extends Hfs
  {
  /** Field name */
  final String name;
  /** Field temporaryPath */
  private String temporaryPath;

  /**
   * Constructor TempDfs creates a new TempDfs instance.
   *
   * @param name of type String
   */
  public TempHfs( String name )
    {
    super( new SequenceFile()
    {
    } ); // source fields setting will be deferred
    this.name = name;
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
  public Scope outgoingScopeFor( Set<Scope> incoming )
    {
    Scope scope = incoming.iterator().next();
    return new Scope( scope.getOutGroupingFields() );
    }

  @Override
  public void sourceInit( JobConf conf ) throws IOException
    {
    makeTemporaryFile( conf );
    super.sourceInit( conf );
    }

  @Override
  public void sinkInit( JobConf conf ) throws IOException
    {
    makeTemporaryFile( conf );
    super.sinkInit( conf );
    }

  @Override
  public boolean isUseTapCollector()
    {
    return false;
    }

  @Override
  public boolean deletePath( JobConf conf ) throws IOException
    {
    return super.deletePath( conf ) && getFileSystem( conf ).delete( new Path( getTempPath( conf ), temporaryPath ) );
    }

  @Override
  public String toString()
    {
    return getClass().getSimpleName() + "[" + name + "]";
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
