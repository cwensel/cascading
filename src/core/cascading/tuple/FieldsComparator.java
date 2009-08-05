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

package cascading.tuple;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Class FieldsComparator is a sub-class of {@link Fields}. It allows a custom {@link Comparator}
 * to be associated with each element position.
 * <p/>
 * Use FieldsComparator instances with the {@link cascading.pipe.GroupBy} pipe to control
 * the grouping and sort orders.
 *
 * @see cascading.pipe.GroupBy
 */
public class FieldsComparator extends Fields implements Comparator<Tuple>
  {
  /** Field comparators */
  private Comparator[] comparators;

  /**
   * Constructor FieldsComparator creates a new FieldsComparator instance.
   *
   * @param fields of type Comparable...
   */
  public FieldsComparator( Comparable... fields )
    {
    super( fields );

    comparators = new Comparator[fields.length];
    }

  /**
   * Method setComparator should be used to associate a {@link Comparator} with a given field name or position.
   *
   * @param fieldName  of type Comparable
   * @param comparator of type Comparator
   */
  public void setComparator( Comparable fieldName, Comparator comparator )
    {
    if( !( comparator instanceof Serializable ) )
      throw new IllegalArgumentException( "given comparator must be serializable" );

    try
      {
      comparators[ getPos( fieldName ) ] = comparator;
      }
    catch( FieldsResolverException exception )
      {
      throw new IllegalArgumentException( "given field name was not found: " + fieldName, exception );
      }
    }

  /**
   * Method setComparators sets all the comparators of this FieldsComparator object. The Comparator array
   * must be the same length as the number for fields in this instance.
   *
   * @param comparators the comparators of this FieldsComparator object.
   */
  public void setComparators( Comparator... comparators )
    {
    if( comparators.length != size() )
      throw new IllegalArgumentException( "given number of comparator instances must match fields size" );

    for( Comparator comparator : comparators )
      {
      if( !( comparator instanceof Serializable ) )
        throw new IllegalArgumentException( "comparators must be serializable" );
      }

    this.comparators = comparators;
    }

  @Override
  public int compare( Tuple lhs, Tuple rhs )
    {
    return lhs.compareTo( comparators, rhs );
    }
  }
