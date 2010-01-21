/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.hadoop;

import java.io.IOException;
import java.util.Comparator;

import cascading.CascadingException;
import cascading.tuple.IndexTuple;
import cascading.tuple.Tuple;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;

/** Class GroupingComparator is an implementation of {@link org.apache.hadoop.io.RawComparator}. */
public class CoGroupingComparator extends DeserializerComparator<IndexTuple>
  {
  private Comparator comparator = new Comparator<Tuple>()
  {
  @Override
  public int compare( Tuple lhs, Tuple rhs )
    {
    return lhs.compareTo( rhs );
    }
  };

  @Override
  public void setConf( Configuration conf )
    {
    super.setConf( conf );

    if( conf == null )
      return;

    String group = conf.get( "cascading.group.comparator" );

    if( group == null )
      return;

    try
      {
      if( group != null )
        comparator = (Comparator) Util.deserializeBase64( group );
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to deserialize grouping comparator" );
      }
    }

  void setDeserializer( TupleSerialization tupleSerialization ) throws IOException
    {
    setDeserializer( tupleSerialization.getIndexTupleDeserializer() );
    }

  public int compare( IndexTuple lhs, IndexTuple rhs )
    {
    return comparator.compare( lhs.getTuple(), rhs.getTuple() );
    }
  }