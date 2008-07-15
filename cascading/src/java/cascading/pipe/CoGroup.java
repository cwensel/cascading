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

package cascading.pipe;

import cascading.pipe.cogroup.CoGrouper;
import cascading.pipe.cogroup.InnerJoin;
import cascading.tuple.Fields;

/**
 * The CoGroup pipe allows for two or more tuple streams to join into a single stream.
 * <p/>
 * For every incoming {@link Pipe} instance, a {@link Fields} instance must be specified that denotes the field names
 * or positions that should be co-grouped with the other given Pipe instances. If the incoming Pipe instances declare
 * one or more field with the same name, the declaredFields must be given to name the outgoing Tuple stream fields.
 * <p/>
 * By default CoGroup performs an Inner Join via the {@link InnerJoin} {@link CoGrouper} class.
 */
public class CoGroup extends Group
  {
  /**
   * Constructor Group creates a new Group instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   */
  public CoGroup( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields )
    {
    super( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   * @param coGrouper      of type CoGrouper
   */
  public CoGroup( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, CoGrouper coGrouper )
    {
    super( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, coGrouper );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param coGrouper      of type CoGrouper
   */
  public CoGroup( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, CoGrouper coGrouper )
    {
    super( lhs, lhsGroupFields, rhs, rhsGroupFields, coGrouper );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   */
  public CoGroup( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields )
    {
    super( lhs, lhsGroupFields, rhs, rhsGroupFields );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipes of type Pipe...
   */
  public CoGroup( Pipe... pipes )
    {
    super( pipes );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipes       of type Pipe[]
   * @param groupFields of type Fields[]
   */
  public CoGroup( Pipe[] pipes, Fields[] groupFields )
    {
    super( pipes, groupFields );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupFields    of type Fields[]
   * @param declaredFields of type Fields
   * @param coGrouper      of type CoGrouper
   */
  public CoGroup( Pipe[] pipes, Fields[] groupFields, Fields declaredFields, CoGrouper coGrouper )
    {
    super( pipes, groupFields, declaredFields, coGrouper );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName   of type String
   * @param pipes       of type Pipe[]
   * @param groupFields of type Fields[]
   */
  public CoGroup( String groupName, Pipe[] pipes, Fields[] groupFields )
    {
    super( groupName, pipes, groupFields );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName      of type String
   * @param pipes          of type Pipe[]
   * @param groupFields    of type Fields[]
   * @param declaredFields of type Fields
   * @param coGrouper      of type CoGrouper
   */
  public CoGroup( String groupName, Pipe[] pipes, Fields[] groupFields, Fields declaredFields, CoGrouper coGrouper )
    {
    super( groupName, pipes, groupFields, declaredFields, coGrouper );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName      of type String
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   */
  public CoGroup( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields )
    {
    super( groupName, lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName      of type String
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   * @param coGrouper      of type CoGrouper
   */
  public CoGroup( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, CoGrouper coGrouper )
    {
    super( groupName, lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, coGrouper );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName      of type String
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param coGrouper      of type CoGrouper
   */
  public CoGroup( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, CoGrouper coGrouper )
    {
    super( groupName, lhs, lhsGroupFields, rhs, rhsGroupFields, coGrouper );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName      of type String
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   */
  public CoGroup( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields )
    {
    super( groupName, lhs, lhsGroupFields, rhs, rhsGroupFields );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName of type String
   * @param pipes     of type Pipe...
   */
  public CoGroup( String groupName, Pipe... pipes )
    {
    super( groupName, pipes );
    }
  }
