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

package cascading.pipe.cogroup;

import java.util.Iterator;
import java.util.Map;

import cascading.tuple.SpillableTupleList;
import cascading.tuple.Tuple;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/** Class CoGroupClosure ... */
public class CoGroupClosure extends GroupClosure
  {
  public static final String SPILL_THRESHOLD = "cascading.spill.threshold";
  private static final int defaultThreshold = 10 * 1000;

  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( CoGroupClosure.class );

  /** Field groups */
  SpillableTupleList[] groups;

  public CoGroupClosure( JobConf jobConf, Map<String, Integer> pipePos, int repeat, Tuple key, Iterator values )
    {
    super( key, values );
    build( jobConf, pipePos, repeat );
    }

  @Override
  public int size()
    {
    return groups.length;
    }

  @Override
  public Iterator<Tuple> getIterator( int pos )
    {
    if( pos < 0 || pos >= groups.length )
      throw new IllegalArgumentException( "invalid group position: " + pos );

    return groups[ pos ].iterator();
    }

  public void build( JobConf jobConf, Map<String, Integer> pipePos, int repeat )
    {
    groups = new SpillableTupleList[Math.max( pipePos.size(), repeat )];

    for( int i = 0; i < pipePos.size(); i++ )
      groups[ i ] = new SpillableTupleList( jobConf.getInt( SPILL_THRESHOLD, defaultThreshold ) );

    while( values.hasNext() )
      {
      Tuple current = (Tuple) values.next();
      String name = (String) current.get( 0 );
      Integer pos = pipePos.get( name ); // if in repeat mode, will always be 0

      if( LOG.isDebugEnabled() )
        {
        LOG.debug( "name: " + name + " pos: " + pos );

        if( repeat != 1 )
          LOG.debug( "repeating: " + repeat );
        }

      groups[ pos ].add( (Tuple) current.get( 1 ) ); // get the value tuple, skipping over the name
      }

    for( int i = 1; i < repeat; i++ )
      groups[ i ] = groups[ 0 ];
    }
  }
