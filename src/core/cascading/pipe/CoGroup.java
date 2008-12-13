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

import cascading.pipe.cogroup.InnerJoin;
import cascading.pipe.cogroup.Joiner;
import cascading.tuple.Fields;

/**
 * The CoGroup pipe allows for two or more tuple streams to join into a single stream.
 * <p/>
 * For every incoming {@link Pipe} instance, a {@link Fields} instance must be specified that denotes the field names
 * or positions that should be co-grouped with the other given Pipe instances. If the incoming Pipe instances declare
 * one or more field with the same name, the declaredFields must be given to name the outgoing Tuple stream fields
 * to overcome field name collisions.
 * <p/>
 * By default CoGroup performs an inner join via the {@link InnerJoin} {@link cascading.pipe.cogroup.Joiner} class.
 * <p/>
 * Self joins an be achieved by using a constructor that takes a single Pipe and a numSelfJoins value. A value of
 * 1 for numSelfJoins will join the Pipe with itself once.
 *
 * @see cascading.pipe.cogroup.InnerJoin
 * @see cascading.pipe.cogroup.OuterJoin
 * @see cascading.pipe.cogroup.LeftJoin
 * @see cascading.pipe.cogroup.RightJoin
 * @see cascading.pipe.cogroup.MixedJoin
 */
public class CoGroup extends Group
  {
  /**
   * Constructor CoGroup creates a new CoGroup instance.
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
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  public CoGroup( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Joiner joiner )
    {
    super( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param joiner         of type CoGrouper
   */
  public CoGroup( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Joiner joiner )
    {
    super( lhs, lhsGroupFields, rhs, rhsGroupFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
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
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param pipes of type Pipe...
   */
  public CoGroup( Pipe... pipes )
    {
    super( pipes );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param pipes       of type Pipe[]
   * @param groupFields of type Fields[]
   */
  public CoGroup( Pipe[] pipes, Fields[] groupFields )
    {
    super( pipes, groupFields );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupFields    of type Fields[]
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  public CoGroup( Pipe[] pipes, Fields[] groupFields, Fields declaredFields, Joiner joiner )
    {
    super( pipes, groupFields, declaredFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
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
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param groupName      of type String
   * @param pipes          of type Pipe[]
   * @param groupFields    of type Fields[]
   * @param declaredFields of type Fields
   */
  public CoGroup( String groupName, Pipe[] pipes, Fields[] groupFields, Fields declaredFields )
    {
    super( groupName, pipes, groupFields, declaredFields );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param groupName      of type String
   * @param pipes          of type Pipe[]
   * @param groupFields    of type Fields[]
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  public CoGroup( String groupName, Pipe[] pipes, Fields[] groupFields, Fields declaredFields, Joiner joiner )
    {
    super( groupName, pipes, groupFields, declaredFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
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
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param groupName      of type String
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  public CoGroup( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Joiner joiner )
    {
    super( groupName, lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param groupName      of type String
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param joiner         of type CoGrouper
   */
  public CoGroup( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Joiner joiner )
    {
    super( groupName, lhs, lhsGroupFields, rhs, rhsGroupFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
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
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param groupName of type String
   * @param pipes     of type Pipe...
   */
  public CoGroup( String groupName, Pipe... pipes )
    {
    super( groupName, pipes );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance that performs numSelfJoins number of self joins on the
   * given {@link Pipe} instance.
   *
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param numSelfJoins         of type int
   * @param declaredFields of type Fields
   */
  public CoGroup( Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields )
    {
    super( pipe, groupFields, numSelfJoins, declaredFields );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance that performs numSelfJoins number of self joins on the
   * given {@link Pipe} instance.
   *
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param numSelfJoins         of type int
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  public CoGroup( Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Joiner joiner )
    {
    super( pipe, groupFields, numSelfJoins, declaredFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance that performs numSelfJoins number of self joins on the
   * given {@link Pipe} instance.
   *
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param numSelfJoins      of type int
   * @param joiner      of type CoGrouper
   */
  public CoGroup( Pipe pipe, Fields groupFields, int numSelfJoins, Joiner joiner )
    {
    super( pipe, groupFields, numSelfJoins, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance that performs numSelfJoins number of self joins on the
   * given {@link Pipe} instance.
   *
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param numSelfJoins      of type int
   */
  public CoGroup( Pipe pipe, Fields groupFields, int numSelfJoins )
    {
    super( pipe, groupFields, numSelfJoins );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance that performs numSelfJoins number of self joins on the
   * given {@link Pipe} instance.
   *
   * @param groupName      of type String
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param numSelfJoins         of type int
   * @param declaredFields of type Fields
   */
  public CoGroup( String groupName, Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields )
    {
    super( groupName, pipe, groupFields, numSelfJoins, declaredFields );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance that performs numSelfJoins number of self joins on the
   * given {@link Pipe} instance.
   *
   * @param groupName      of type String
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param numSelfJoins         of type int
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  public CoGroup( String groupName, Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Joiner joiner )
    {
    super( groupName, pipe, groupFields, numSelfJoins, declaredFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance that performs numSelfJoins number of self joins on the
   * given {@link Pipe} instance.
   *
   * @param groupName   of type String
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param numSelfJoins      of type int
   * @param joiner      of type CoGrouper
   */
  public CoGroup( String groupName, Pipe pipe, Fields groupFields, int numSelfJoins, Joiner joiner )
    {
    super( groupName, pipe, groupFields, numSelfJoins, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance that performs numSelfJoins number of self joins on the
   * given {@link Pipe} instance.
   *
   * @param groupName   of type String
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param numSelfJoins      of type int
   */
  public CoGroup( String groupName, Pipe pipe, Fields groupFields, int numSelfJoins )
    {
    super( groupName, pipe, groupFields, numSelfJoins );
    }
  }
