/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.pipe;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.planner.Scope;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.Joiner;
import cascading.tuple.Fields;
import cascading.tuple.FieldsResolverException;
import cascading.tuple.TupleException;
import cascading.tuple.coerce.Coercions;
import cascading.util.Util;

import static java.util.Arrays.asList;

/**
 * The base class for {@link GroupBy}, {@link CoGroup}, {@link Merge}, and {@link HashJoin}. This class should not be used directly.
 *
 * @see GroupBy
 * @see CoGroup
 * @see Merge
 * @see HashJoin
 */
public class Splice extends Pipe
  {
  static enum Kind
    {
      GroupBy, CoGroup, Merge, Join
    }

  private Kind kind;
  /** Field spliceName */
  private String spliceName;
  /** Field pipes */
  private final List<Pipe> pipes = new ArrayList<Pipe>();
  /** Field groupFieldsMap */
  protected final Map<String, Fields> keyFieldsMap = new LinkedHashMap<String, Fields>(); // keep order
  /** Field sortFieldsMap */
  protected Map<String, Fields> sortFieldsMap = new LinkedHashMap<String, Fields>(); // keep order
  /** Field reverseOrder */
  private boolean reverseOrder = false;
  /** Field declaredFields */
  protected Fields declaredFields;
  /** Field resultGroupFields */
  protected Fields resultGroupFields;
  /** Field repeat */
  private int numSelfJoins = 0;
  /** Field coGrouper */
  private Joiner joiner;

  /** Field pipePos */
  private transient Map<String, Integer> pipePos;

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   */
  protected Splice( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, null, null );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param lhs               of type Pipe
   * @param lhsGroupFields    of type Fields
   * @param rhs               of type Pipe
   * @param rhsGroupFields    of type Fields
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   */
  protected Splice( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Fields resultGroupFields )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, resultGroupFields, null );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Splice( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Joiner joiner )
    {
    this( Pipe.pipes( lhs, rhs ), Fields.fields( lhsGroupFields, rhsGroupFields ), declaredFields, joiner );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param lhs               of type Pipe
   * @param lhsGroupFields    of type Fields
   * @param rhs               of type Pipe
   * @param rhsGroupFields    of type Fields
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   * @param joiner            of type Joiner
   */
  protected Splice( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Fields resultGroupFields, Joiner joiner )
    {
    this( Pipe.pipes( lhs, rhs ), Fields.fields( lhsGroupFields, rhsGroupFields ), declaredFields, resultGroupFields, joiner );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Splice( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Joiner joiner )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, null, joiner );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   */
  protected Splice( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields )
    {
    this( Pipe.pipes( lhs, rhs ), Fields.fields( lhsGroupFields, rhsGroupFields ) );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipes of type Pipe...
   */
  protected Splice( Pipe... pipes )
    {
    this( pipes, (Fields[]) null );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipes       of type Pipe[]
   * @param groupFields of type Fields[]
   */
  protected Splice( Pipe[] pipes, Fields[] groupFields )
    {
    this( null, pipes, groupFields, null, null );
    }


  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName  of type String
   * @param pipes       of type Pipe[]
   * @param groupFields of type Fields[]
   */
  protected Splice( String spliceName, Pipe[] pipes, Fields[] groupFields )
    {
    this( spliceName, pipes, groupFields, null, null );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName     of type String
   * @param pipes          of type Pipe[]
   * @param groupFields    of type Fields[]
   * @param declaredFields of type Fields
   */
  protected Splice( String spliceName, Pipe[] pipes, Fields[] groupFields, Fields declaredFields )
    {
    this( spliceName, pipes, groupFields, declaredFields, null );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName        of type String
   * @param pipes             of type Pipe[]
   * @param groupFields       of type Fields[]
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   */
  protected Splice( String spliceName, Pipe[] pipes, Fields[] groupFields, Fields declaredFields, Fields resultGroupFields )
    {
    this( spliceName, pipes, groupFields, declaredFields, resultGroupFields, null );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupFields    of type Fields[]
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Splice( Pipe[] pipes, Fields[] groupFields, Fields declaredFields, Joiner joiner )
    {
    this( null, pipes, groupFields, declaredFields, null, joiner );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipes             of type Pipe[]
   * @param groupFields       of type Fields[]
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   * @param joiner            of type Joiner
   */
  protected Splice( Pipe[] pipes, Fields[] groupFields, Fields declaredFields, Fields resultGroupFields, Joiner joiner )
    {
    this( null, pipes, groupFields, declaredFields, resultGroupFields, joiner );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName     of type String
   * @param pipes          of type Pipe[]
   * @param groupFields    of type Fields[]
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Splice( String spliceName, Pipe[] pipes, Fields[] groupFields, Fields declaredFields, Fields resultGroupFields, Joiner joiner )
    {
    setKind();
    this.spliceName = spliceName;

    int uniques = new HashSet<Pipe>( asList( Pipe.resolvePreviousAll( pipes ) ) ).size();

    if( pipes.length > 1 && uniques == 1 )
      {
      if( new HashSet<Fields>( asList( groupFields ) ).size() != 1 )
        throw new IllegalArgumentException( "all groupFields must be identical" );

      addPipe( pipes[ 0 ] );
      this.numSelfJoins = pipes.length - 1;
      this.keyFieldsMap.put( pipes[ 0 ].getName(), groupFields[ 0 ] );

      if( resultGroupFields != null && groupFields[ 0 ].size() * pipes.length != resultGroupFields.size() )
        throw new IllegalArgumentException( "resultGroupFields and cogroup joined fields must be same size" );
      }
    else
      {
      int last = -1;
      for( int i = 0; i < pipes.length; i++ )
        {
        addPipe( pipes[ i ] );

        if( groupFields == null || groupFields.length == 0 )
          {
          addGroupFields( pipes[ i ], Fields.FIRST );
          continue;
          }

        if( last != -1 && last != groupFields[ i ].size() )
          throw new IllegalArgumentException( "all groupFields must be same size" );

        last = groupFields[ i ].size();
        addGroupFields( pipes[ i ], groupFields[ i ] );
        }

      if( resultGroupFields != null && last * pipes.length != resultGroupFields.size() )
        throw new IllegalArgumentException( "resultGroupFields and cogroup resulting joined fields must be same size" );
      }

    this.declaredFields = declaredFields;
    this.resultGroupFields = resultGroupFields;
    this.joiner = joiner;

    verifyCoGrouper();
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName     of type String
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   */
  protected Splice( String spliceName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields );
    this.spliceName = spliceName;
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName        of type String
   * @param lhs               of type Pipe
   * @param lhsGroupFields    of type Fields
   * @param rhs               of type Pipe
   * @param rhsGroupFields    of type Fields
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   */
  protected Splice( String spliceName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Fields resultGroupFields )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, resultGroupFields );
    this.spliceName = spliceName;
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName     of type String
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Splice( String spliceName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Joiner joiner )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, joiner );
    this.spliceName = spliceName;
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName        of type String
   * @param lhs               of type Pipe
   * @param lhsGroupFields    of type Fields
   * @param rhs               of type Pipe
   * @param rhsGroupFields    of type Fields
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   * @param joiner            of type Joiner
   */
  protected Splice( String spliceName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Fields resultGroupFields, Joiner joiner )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, resultGroupFields, joiner );
    this.spliceName = spliceName;
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName     of type String
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Splice( String spliceName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Joiner joiner )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, joiner );
    this.spliceName = spliceName;
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName     of type String
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   */
  protected Splice( String spliceName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields );
    this.spliceName = spliceName;
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName of type String
   * @param pipes      of type Pipe...
   */
  protected Splice( String spliceName, Pipe... pipes )
    {
    this( pipes );
    this.spliceName = spliceName;
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param numSelfJoins   of type int
   * @param declaredFields of type Fields
   */
  protected Splice( Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields )
    {
    this( pipe, groupFields, numSelfJoins );
    this.declaredFields = declaredFields;
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipe              of type Pipe
   * @param groupFields       of type Fields
   * @param numSelfJoins      of type int
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   */
  protected Splice( Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Fields resultGroupFields )
    {
    this( pipe, groupFields, numSelfJoins );
    this.declaredFields = declaredFields;
    this.resultGroupFields = resultGroupFields;

    if( resultGroupFields != null && groupFields.size() * numSelfJoins != resultGroupFields.size() )
      throw new IllegalArgumentException( "resultGroupFields and cogroup resulting join fields must be same size" );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param numSelfJoins   of type int
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Splice( Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Joiner joiner )
    {
    this( pipe, groupFields, numSelfJoins, declaredFields );
    this.joiner = joiner;

    verifyCoGrouper();
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipe              of type Pipe
   * @param groupFields       of type Fields
   * @param numSelfJoins      of type int
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   * @param joiner            of type Joiner
   */
  protected Splice( Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Fields resultGroupFields, Joiner joiner )
    {
    this( pipe, groupFields, numSelfJoins, declaredFields, resultGroupFields );
    this.joiner = joiner;

    verifyCoGrouper();
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param numSelfJoins of type int
   * @param joiner       of type CoGrouper
   */
  protected Splice( Pipe pipe, Fields groupFields, int numSelfJoins, Joiner joiner )
    {
    setKind();
    addPipe( pipe );
    this.keyFieldsMap.put( pipe.getName(), groupFields );
    this.numSelfJoins = numSelfJoins;
    this.joiner = joiner;

    verifyCoGrouper();
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param numSelfJoins of type int
   */
  protected Splice( Pipe pipe, Fields groupFields, int numSelfJoins )
    {
    this( pipe, groupFields, numSelfJoins, (Joiner) null );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName     of type String
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param numSelfJoins   of type int
   * @param declaredFields of type Fields
   */
  protected Splice( String spliceName, Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields )
    {
    this( pipe, groupFields, numSelfJoins, declaredFields );
    this.spliceName = spliceName;
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName        of type String
   * @param pipe              of type Pipe
   * @param groupFields       of type Fields
   * @param numSelfJoins      of type int
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   */
  protected Splice( String spliceName, Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Fields resultGroupFields )
    {
    this( pipe, groupFields, numSelfJoins, declaredFields, resultGroupFields );
    this.spliceName = spliceName;
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName     of type String
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param numSelfJoins   of type int
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Splice( String spliceName, Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Joiner joiner )
    {
    this( pipe, groupFields, numSelfJoins, declaredFields, joiner );
    this.spliceName = spliceName;
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName        of type String
   * @param pipe              of type Pipe
   * @param groupFields       of type Fields
   * @param numSelfJoins      of type int
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   * @param joiner            of type Joiner
   */
  protected Splice( String spliceName, Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Fields resultGroupFields, Joiner joiner )
    {
    this( pipe, groupFields, numSelfJoins, declaredFields, resultGroupFields, joiner );
    this.spliceName = spliceName;
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName   of type String
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param numSelfJoins of type int
   * @param joiner       of type CoGrouper
   */
  protected Splice( String spliceName, Pipe pipe, Fields groupFields, int numSelfJoins, Joiner joiner )
    {
    this( pipe, groupFields, numSelfJoins, joiner );
    this.spliceName = spliceName;
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName   of type String
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param numSelfJoins of type int
   */
  protected Splice( String spliceName, Pipe pipe, Fields groupFields, int numSelfJoins )
    {
    this( pipe, groupFields, numSelfJoins );
    this.spliceName = spliceName;
    }

  ////////////
  // GROUPBY
  ////////////

  /**
   * Constructor Splice creates a new Splice instance where grouping occurs on {@link Fields#ALL} fields.
   *
   * @param pipe of type Pipe
   */
  protected Splice( Pipe pipe )
    {
    this( null, pipe, Fields.ALL, null, false );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   */
  protected Splice( Pipe pipe, Fields groupFields )
    {
    this( null, pipe, groupFields, null, false );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName  of type String
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   */
  protected Splice( String spliceName, Pipe pipe, Fields groupFields )
    {
    this( spliceName, pipe, groupFields, null, false );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param sortFields  of type Fields
   */
  protected Splice( Pipe pipe, Fields groupFields, Fields sortFields )
    {
    this( null, pipe, groupFields, sortFields, false );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName  of type String
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param sortFields  of type Fields
   */
  protected Splice( String spliceName, Pipe pipe, Fields groupFields, Fields sortFields )
    {
    this( spliceName, pipe, groupFields, sortFields, false );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param sortFields   of type Fields
   * @param reverseOrder of type boolean
   */
  protected Splice( Pipe pipe, Fields groupFields, Fields sortFields, boolean reverseOrder )
    {
    this( null, pipe, groupFields, sortFields, reverseOrder );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName   of type String
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param sortFields   of type Fields
   * @param reverseOrder of type boolean
   */
  protected Splice( String spliceName, Pipe pipe, Fields groupFields, Fields sortFields, boolean reverseOrder )
    {
    this( spliceName, Pipe.pipes( pipe ), groupFields, sortFields, reverseOrder );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipes       of type Pipe
   * @param groupFields of type Fields
   */
  protected Splice( Pipe[] pipes, Fields groupFields )
    {
    this( null, pipes, groupFields, null, false );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName  of type String
   * @param pipes       of type Pipe
   * @param groupFields of type Fields
   */
  protected Splice( String spliceName, Pipe[] pipes, Fields groupFields )
    {
    this( spliceName, pipes, groupFields, null, false );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipes       of type Pipe
   * @param groupFields of type Fields
   * @param sortFields  of type Fields
   */
  protected Splice( Pipe[] pipes, Fields groupFields, Fields sortFields )
    {
    this( null, pipes, groupFields, sortFields, false );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName  of type String
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param sortFields  of type Fields
   */
  protected Splice( String spliceName, Pipe[] pipe, Fields groupFields, Fields sortFields )
    {
    this( spliceName, pipe, groupFields, sortFields, false );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param pipes        of type Pipe
   * @param groupFields  of type Fields
   * @param sortFields   of type Fields
   * @param reverseOrder of type boolean
   */
  protected Splice( Pipe[] pipes, Fields groupFields, Fields sortFields, boolean reverseOrder )
    {
    this( null, pipes, groupFields, sortFields, reverseOrder );
    }

  /**
   * Constructor Splice creates a new Splice instance.
   *
   * @param spliceName   of type String
   * @param pipes        of type Pipe[]
   * @param groupFields  of type Fields
   * @param sortFields   of type Fields
   * @param reverseOrder of type boolean
   */
  protected Splice( String spliceName, Pipe[] pipes, Fields groupFields, Fields sortFields, boolean reverseOrder )
    {
    setKind();
    this.spliceName = spliceName;

    for( Pipe pipe : pipes )
      {
      addPipe( pipe );
      this.keyFieldsMap.put( pipe.getName(), groupFields );

      if( sortFields != null )
        this.sortFieldsMap.put( pipe.getName(), sortFields );
      }

    this.reverseOrder = reverseOrder;
    this.joiner = new InnerJoin();
    }

  private void verifyCoGrouper()
    {
    if( joiner == null )
      {
      joiner = new InnerJoin();
      return;
      }

    if( joiner.numJoins() == -1 )
      return;

    int joins = Math.max( numSelfJoins, keyFieldsMap.size() - 1 ); // joining two streams is one join

    if( joins != joiner.numJoins() )
      throw new IllegalArgumentException( "invalid joiner, only accepts " + joiner.numJoins() + " joins, there are: " + joins );
    }

  private void setKind()
    {
    if( this instanceof GroupBy )
      kind = Kind.GroupBy;
    else if( this instanceof CoGroup )
      kind = Kind.CoGroup;
    else if( this instanceof Merge )
      kind = Kind.Merge;
    else
      kind = Kind.Join;
    }

  /**
   * Method getDeclaredFields returns the declaredFields of this Splice object.
   *
   * @return the declaredFields (type Fields) of this Splice object.
   */
  public Fields getDeclaredFields()
    {
    return declaredFields;
    }

  private void addPipe( Pipe pipe )
    {
    if( pipe.getName() == null )
      throw new IllegalArgumentException( "each input pipe must have a name" );

    pipes.add( pipe ); // allow same pipe
    }

  private void addGroupFields( Pipe pipe, Fields fields )
    {
    if( keyFieldsMap.containsKey( pipe.getName() ) )
      throw new IllegalArgumentException( "each input pipe branch must be uniquely named" );

    keyFieldsMap.put( pipe.getName(), fields );
    }

  @Override
  public String getName()
    {
    if( spliceName != null )
      return spliceName;

    StringBuffer buffer = new StringBuffer();

    for( Pipe pipe : pipes )
      {
      if( buffer.length() != 0 )
        {
        if( isGroupBy() || isMerge() )
          buffer.append( "+" );
        else if( isCoGroup() || isJoin() )
          buffer.append( "*" ); // more semantically correct
        }

      buffer.append( pipe.getName() );
      }

    spliceName = buffer.toString();

    return spliceName;
    }

  @Override
  public Pipe[] getPrevious()
    {
    return pipes.toArray( new Pipe[ pipes.size() ] );
    }

  /**
   * Method getGroupingSelectors returns the groupingSelectors of this Splice object.
   *
   * @return the groupingSelectors (type Map<String, Fields>) of this Splice object.
   */
  public Map<String, Fields> getKeySelectors()
    {
    return keyFieldsMap;
    }

  /**
   * Method getSortingSelectors returns the sortingSelectors of this Splice object.
   *
   * @return the sortingSelectors (type Map<String, Fields>) of this Splice object.
   */
  public Map<String, Fields> getSortingSelectors()
    {
    return sortFieldsMap;
    }

  /**
   * Method isSorted returns true if this Splice instance is sorting values other than the group fields.
   *
   * @return the sorted (type boolean) of this Splice object.
   */
  public boolean isSorted()
    {
    return !sortFieldsMap.isEmpty();
    }

  /**
   * Method isSortReversed returns true if sorting is reversed.
   *
   * @return the sortReversed (type boolean) of this Splice object.
   */
  public boolean isSortReversed()
    {
    return reverseOrder;
    }

  public synchronized Map<String, Integer> getPipePos()
    {
    if( pipePos != null )
      return pipePos;

    pipePos = new HashMap<String, Integer>();

    int pos = 0;
    for( Object pipe : pipes )
      pipePos.put( ( (Pipe) pipe ).getName(), pos++ );

    return pipePos;
    }

  public Joiner getJoiner()
    {
    return joiner;
    }

  /**
   * Method isGroupBy returns true if this Splice instance will perform a GroupBy operation.
   *
   * @return the groupBy (type boolean) of this Splice object.
   */
  public final boolean isGroupBy()
    {
    return kind == Kind.GroupBy;
    }

  public final boolean isCoGroup()
    {
    return kind == Kind.CoGroup;
    }

  public final boolean isMerge()
    {
    return kind == Kind.Merge;
    }

  public final boolean isJoin()
    {
    return kind == Kind.Join;
    }

  public int getNumSelfJoins()
    {
    return numSelfJoins;
    }

  boolean isSelfJoin()
    {
    return numSelfJoins != 0;
    }

  // FIELDS

  @Override
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    Map<String, Fields> groupingSelectors = resolveGroupingSelectors( incomingScopes );
    Map<String, Fields> sortingSelectors = resolveSortingSelectors( incomingScopes );
    Fields declared = resolveDeclared( incomingScopes );

    Fields outGroupingFields = resultGroupFields;

    if( outGroupingFields == null && isCoGroup() )
      outGroupingFields = createJoinFields( incomingScopes, groupingSelectors, declared );

    // for Group, the outgoing fields are the same as those declared
    return new Scope( getName(), declared, outGroupingFields, groupingSelectors, sortingSelectors, declared, isGroupBy() );
    }

  private Fields createJoinFields( Set<Scope> incomingScopes, Map<String, Fields> groupingSelectors, Fields declared )
    {
    Map<String, Fields> incomingFields = new HashMap<String, Fields>();

    for( Scope scope : incomingScopes )
      incomingFields.put( scope.getName(), scope.getIncomingSpliceFields() );

    Fields outGroupingFields = Fields.NONE;

    int offset = 0;
    for( Pipe pipe : pipes ) // need to retain order of pipes
      {
      String pipeName = pipe.getName();
      Fields pipeGroupingSelector = groupingSelectors.get( pipeName );
      Fields incomingField = incomingFields.get( pipeName );

      if( !pipeGroupingSelector.isNone() )
        {
        Fields offsetFields = incomingField.selectPos( pipeGroupingSelector, offset );
        Fields resolvedSelect = declared.select( offsetFields );

        outGroupingFields = outGroupingFields.append( resolvedSelect );
        }

      offset += incomingField.size();
      }

    return outGroupingFields;
    }

  Map<String, Fields> resolveGroupingSelectors( Set<Scope> incomingScopes )
    {
    try
      {
      Map<String, Fields> groupingSelectors = getKeySelectors();
      Map<String, Fields> groupingFields = resolveSelectorsAgainstIncoming( incomingScopes, groupingSelectors, "grouping" );

      if( !verifySameSize( groupingFields ) )
        throw new OperatorException( this, "all grouping fields must be same size: " + toString() );

      if( !verifySameTypes( groupingSelectors, groupingFields ) )
        throw new OperatorException( this, "all grouping fields must declare same types: " + toString() );

      return groupingFields;
      }
    catch( FieldsResolverException exception )
      {
      throw new OperatorException( this, OperatorException.Kind.grouping, exception.getSourceFields(), exception.getSelectorFields(), exception );
      }
    catch( RuntimeException exception )
      {
      throw new OperatorException( this, "could not resolve grouping selector in: " + this, exception );
      }
    }

  private boolean verifySameTypes( Map<String, Fields> groupingSelectors, Map<String, Fields> groupingFields )
    {
    // create array of field positions with comparators
    // unsure which side has the comparators declared so make a union
    boolean[] hasComparator = new boolean[ groupingFields.values().iterator().next().size() ];

    for( Map.Entry<String, Fields> entry : groupingSelectors.entrySet() )
      {
      Comparator[] comparatorsArray = entry.getValue().getComparators();

      for( int i = 0; i < comparatorsArray.length; i++ )
        hasComparator[ i ] = hasComparator[ i ] || comparatorsArray[ i ] != null;
      }

    Iterator<Fields> iterator = groupingFields.values().iterator();
    Type[] types = iterator.next().getTypes();

    // if types are null, no basis for comparison
    if( types == null )
      return true;

    while( iterator.hasNext() )
      {
      Fields groupingField = iterator.next();
      Type[] groupingTypes = groupingField.getTypes();

      // if types are null, no basis for comparison
      if( groupingTypes == null )
        return true;

      for( int i = 0; i < types.length; i++ )
        {
        if( hasComparator[ i ] )
          continue;

        Type lhs = types[ i ];
        Type rhs = groupingTypes[ i ];

        // if one side is primitive, up-class to its primitive wrapper type
        if( lhs instanceof Class && rhs instanceof Class )
          {
          lhs = Coercions.asNonPrimitive( (Class) lhs );
          rhs = Coercions.asNonPrimitive( (Class) rhs );
          }

        if( !lhs.equals( rhs ) )
          return false;
        }
      }

    return true;
    }

  private boolean verifySameSize( Map<String, Fields> groupingFields )
    {
    Iterator<Fields> iterator = groupingFields.values().iterator();
    int size = iterator.next().size();

    while( iterator.hasNext() )
      {
      Fields groupingField = iterator.next();

      if( groupingField.size() != size )
        return false;

      size = groupingField.size();
      }

    return true;
    }

  private Map<String, Fields> resolveSelectorsAgainstIncoming( Set<Scope> incomingScopes, Map<String, Fields> selectors, String type )
    {
    Map<String, Fields> resolvedFields = new HashMap<String, Fields>();

    for( Scope incomingScope : incomingScopes )
      {
      Fields selector = selectors.get( incomingScope.getName() );

      if( selector == null )
        throw new OperatorException( this, "no " + type + " selector found for: " + incomingScope.getName() );

      Fields incomingFields;

      if( selector.isNone() )
        incomingFields = Fields.NONE;
      else if( selector.isAll() )
        incomingFields = incomingScope.getIncomingSpliceFields();
      else if( selector.isGroup() )
        incomingFields = incomingScope.getOutGroupingFields();
      else if( selector.isValues() )
        incomingFields = incomingScope.getOutValuesFields().subtract( incomingScope.getOutGroupingFields() );
      else
        incomingFields = incomingScope.getIncomingSpliceFields().select( selector );

      resolvedFields.put( incomingScope.getName(), incomingFields );
      }

    return resolvedFields;
    }

  Map<String, Fields> resolveSortingSelectors( Set<Scope> incomingScopes )
    {
    try
      {
      if( getSortingSelectors().isEmpty() )
        return null;

      return resolveSelectorsAgainstIncoming( incomingScopes, getSortingSelectors(), "sorting" );
      }
    catch( FieldsResolverException exception )
      {
      throw new OperatorException( this, OperatorException.Kind.sorting, exception.getSourceFields(), exception.getSelectorFields(), exception );
      }
    catch( RuntimeException exception )
      {
      throw new OperatorException( this, "could not resolve sorting selector in: " + this, exception );
      }
    }

  @Override
  public Fields resolveIncomingOperationPassThroughFields( Scope incomingScope )
    {
    return incomingScope.getIncomingSpliceFields();
    }

  Fields resolveDeclared( Set<Scope> incomingScopes )
    {
    try
      {
      Fields declaredFields = getDeclaredFields();

      if( declaredFields != null ) // null for GroupBy
        {
        if( incomingScopes.size() != pipes.size() && isSelfJoin() )
          throw new OperatorException( this, "self joins without intermediate operators are not permitted, see 'numSelfJoins' constructor or identity function" );

        int size = 0;
        boolean foundUnknown = false;

        List<Fields> appendableFields = getOrderedResolvedFields( incomingScopes );

        for( Fields fields : appendableFields )
          {
          foundUnknown = foundUnknown || fields.isUnknown();
          size += fields.size();
          }

        // we must relax field checking in the face of unkown fields
        if( !foundUnknown && declaredFields.size() != size * ( numSelfJoins + 1 ) )
          {
          if( isSelfJoin() )
            throw new OperatorException( this, "declared grouped fields not same size as grouped values, declared: " + declaredFields.printVerbose() + " != size: " + size * ( numSelfJoins + 1 ) );
          else
            throw new OperatorException( this, "declared grouped fields not same size as grouped values, declared: " + declaredFields.printVerbose() + " resolved: " + Util.print( appendableFields, "" ) );
          }

        int i = 0;
        for( Fields appendableField : appendableFields )
          {
          Type[] types = appendableField.getTypes();

          if( types == null )
            {
            i += appendableField.size();
            continue;
            }

          for( Type type : types )
            {
            if( type != null )
              declaredFields = declaredFields.applyType( i, type );

            i++;
            }
          }

        return declaredFields;
        }

      // support merge or cogrouping here
      if( isGroupBy() || isMerge() )
        {
        Iterator<Scope> iterator = incomingScopes.iterator();
        Fields commonFields = iterator.next().getIncomingSpliceFields();

        while( iterator.hasNext() )
          {
          Scope incomingScope = iterator.next();
          Fields fields = incomingScope.getIncomingSpliceFields();

          if( !commonFields.equalsFields( fields ) )
            throw new OperatorException( this, "merged streams must declare the same field names, expected: " + commonFields.printVerbose() + " found: " + fields.printVerbose() );
          }

        return commonFields;
        }
      else
        {
        List<Fields> appendableFields = getOrderedResolvedFields( incomingScopes );
        Fields appendedFields = new Fields();

        try
          {
          // will fail on name collisions
          for( Fields appendableField : appendableFields )
            appendedFields = appendedFields.append( appendableField );
          }
        catch( TupleException exception )
          {
          String fields = "";

          for( Fields appendableField : appendableFields )
            fields += appendableField.print();

          throw new OperatorException( this, "found duplicate field names in joined tuple stream: " + fields, exception );
          }

        return appendedFields;
        }
      }
    catch( OperatorException exception )
      {
      throw exception;
      }
    catch( RuntimeException exception )
      {
      throw new OperatorException( this, "could not resolve declared fields in: " + this, exception );
      }
    }

  private List<Fields> getOrderedResolvedFields( Set<Scope> incomingScopes )
    {
    Map<String, Scope> scopesMap = new HashMap<String, Scope>();

    for( Scope incomingScope : incomingScopes )
      scopesMap.put( incomingScope.getName(), incomingScope );

    List<Fields> appendableFields = new ArrayList<Fields>();

    for( Pipe pipe : pipes )
      appendableFields.add( scopesMap.get( pipe.getName() ).getIncomingSpliceFields() );
    return appendableFields;
    }

  @Override
  public boolean isEquivalentTo( FlowElement element )
    {
    boolean equivalentTo = super.isEquivalentTo( element );

    if( !equivalentTo )
      return equivalentTo;

    Splice splice = (Splice) element;

    if( !keyFieldsMap.equals( splice.keyFieldsMap ) )
      return false;

    if( !pipes.equals( splice.pipes ) )
      return false;

    return true;
    }

  // OBJECT OVERRIDES

  @Override
  @SuppressWarnings({"RedundantIfStatement"})
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;
    if( !super.equals( object ) )
      return false;

    Splice splice = (Splice) object;

    if( spliceName != null ? !spliceName.equals( splice.spliceName ) : splice.spliceName != null )
      return false;
    if( keyFieldsMap != null ? !keyFieldsMap.equals( splice.keyFieldsMap ) : splice.keyFieldsMap != null )
      return false;
    if( pipes != null ? !pipes.equals( splice.pipes ) : splice.pipes != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( pipes != null ? pipes.hashCode() : 0 );
    result = 31 * result + ( keyFieldsMap != null ? keyFieldsMap.hashCode() : 0 );
    result = 31 * result + ( spliceName != null ? spliceName.hashCode() : 0 );
    return result;
    }

  @Override
  public String toString()
    {
    StringBuilder buffer = new StringBuilder( super.toString() );

    buffer.append( "[by:" );

    for( String name : keyFieldsMap.keySet() )
      {
      if( keyFieldsMap.size() > 1 )
        buffer.append( " " ).append( name ).append( ":" );

      buffer.append( keyFieldsMap.get( name ).printVerbose() );
      }

    if( isSelfJoin() )
      buffer.append( "[numSelfJoins:" ).append( numSelfJoins ).append( "]" );

    buffer.append( "]" );

    return buffer.toString();
    }

  @Override
  protected void printInternal( StringBuffer buffer, Scope scope )
    {
    super.printInternal( buffer, scope );
    Map<String, Fields> map = scope.getKeySelectors();

    if( map != null )
      {
      buffer.append( "[by:" );

      // important to retain incoming pipe order
      for( Map.Entry<String, Fields> entry : keyFieldsMap.entrySet() )
        {
        String name = entry.getKey();

        if( map.size() > 1 )
          buffer.append( name ).append( ":" );

        buffer.append( map.get( name ).print() ); // get resolved keys
        }

      if( isSelfJoin() )
        buffer.append( "[numSelfJoins:" ).append( numSelfJoins ).append( "]" );

      buffer.append( "]" );
      }
    }
  }
