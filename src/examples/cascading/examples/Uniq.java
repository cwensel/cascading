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

package cascading.examples;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import cascading.flow.Scope;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/** Class Uniq ... */
public class Uniq extends Group
  {
  private static final Logger LOG = Logger.getLogger( Uniq.class );
  private static final Tuple EMPTY = new Tuple();

  public Uniq( Pipe pipe )
    {
    super( pipe, Fields.ALL );
    }

  public Uniq( Pipe pipe, Fields groupFields )
    {
    super( pipe, groupFields );
    }

  @Override
  public Tuple[] makeReduceGrouping( Scope incomingScope, Scope outgoingScope, TupleEntry entry )
    {
    Fields groupFields = groupFieldsMap.get( incomingScope.getName() );

    if( LOG.isDebugEnabled() )
      LOG.debug( "uniq: [" + incomingScope + "] key pos: [" + groupFields + "]" );

    if( groupFields.isAll() )
      return new Tuple[]{entry.getTuple(), EMPTY};
    else
      return new Tuple[]{entry.selectTuple( groupFields ), EMPTY};
    }

  @Override
  public Iterator<Tuple> makeReduceValues( JobConf jobConf, Set<Scope> incomingScopes, Scope thisScope, WritableComparable key, Iterator values )
    {
    // we must walk all the values, otherwise we will get dupe keys
    while( values.hasNext() )
      values.next();

    return Collections.singletonList( (Tuple) key ).iterator();
    }
  }
