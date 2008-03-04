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

import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;

/**
 * Class MultiTap is used to tie multipe {@link Tap} instances into a single resource. Effectively this will allow
 * multiple files to be concatenated into the requesting pipe assembly.
 * <p/>
 * Note that order is not maintained by virtue of the underlying model. If order is necessary, use a unique sequence key
 * to span the resources, like a line number.
 */
public class MultiTap extends SourceTap
  {
  protected Tap[] taps;

  protected MultiTap( Scheme scheme )
    {
    super( scheme );
    }

  public MultiTap( Tap... taps )
    {
    this.taps = taps;

    verifyTaps();
    }

  private void verifyTaps()
    {
    Tap tap = taps[ 0 ];

    for( int i = 1; i < taps.length; i++ )
      {
      if( tap.getClass() != taps[ i ].getClass() )
        throw new TapException( "all taps must be of the same type" );

      if( !tap.getScheme().equals( taps[ i ].getScheme() ) )
        throw new TapException( "all tap schemes must be equivalent" );
      }
    }

  public Path getPath()
    {
    return null;
    }

  @Override
  public Fields getSourceFields()
    {
    return taps[ 0 ].getSourceFields();
    }

  @Override
  public void sourceInit( JobConf conf ) throws IOException
    {
    for( Tap tap : taps )
      tap.sourceInit( conf );
    }

  public Tap findTapFor( JobConf conf, String currentFile )
    {
    for( Tap tap : taps )
      {
      if( tap.containsFile( conf, currentFile ) )
        return tap;
      }

    return null;
    }

  public boolean containsFile( JobConf conf, String currentFile )
    {
    return findTapFor( conf, currentFile ) != null;
    }

  @Override
  public Tuple source( WritableComparable key, Writable value )
    {
    return taps[ 0 ].source( key, value );
    }

  public boolean pathExists( JobConf conf ) throws IOException
    {
    for( Tap tap : taps )
      {
      if( tap.pathExists( conf ) )
        return true;
      }

    return false;
    }

  /** Returns the most current modified time. */
  public long getPathModified( JobConf conf ) throws IOException
    {
    long modified = taps[ 0 ].getPathModified( conf );

    for( int i = 1; i < taps.length; i++ )
      modified = Math.max( taps[ i ].getPathModified( conf ), modified );

    return modified;
    }
  }
