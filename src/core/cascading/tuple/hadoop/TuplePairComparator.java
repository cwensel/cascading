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

package cascading.tuple.hadoop;

import java.io.IOException;
import java.util.Comparator;

import cascading.CascadingException;
import cascading.tuple.TuplePair;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;

/** Class TuplePairComparator is an implementation of {@link org.apache.hadoop.io.RawComparator}. */
public class TuplePairComparator extends DeserializerComparator<TuplePair>
  {
  private Comparator comparator = new Comparator<TuplePair>()
  {
  @Override
  public int compare( TuplePair lhs, TuplePair rhs )
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
    String sort = conf.get( "cascading.sort.comparator" );

    if( sort == null && group == null )
      return;

    Comparator groupComparator = comparator;
    Comparator sortComparator = comparator;

    try
      {
      if( group != null )
        groupComparator = (Comparator) Util.deserializeBase64( group );
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to deserialize grouping comparator" );
      }

    try
      {
      if( sort != null )
        sortComparator = (Comparator) Util.deserializeBase64( sort );
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to deserialize sorting comparator" );
      }

    final Comparator finalGroupComparator = groupComparator;
    final Comparator finalSortComparator = sortComparator;

    comparator = new Comparator<TuplePair>()
    {
    @Override
    public int compare( TuplePair lhs, TuplePair rhs )
      {
      int c = finalGroupComparator.compare( lhs.getLhs(), rhs.getLhs() );

      if( c != 0 )
        return c;

      return finalSortComparator.compare( lhs.getRhs(), rhs.getRhs() );
      }
    };
    }


  void setDeserializer( TupleSerialization tupleSerialization ) throws IOException
    {
    setDeserializer( tupleSerialization.getTuplePairDeserializer() );
    }

  public int compare( TuplePair lhs, TuplePair rhs )
    {
    return comparator.compare( lhs, rhs );
    }
  }