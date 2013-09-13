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

import java.beans.ConstructorProperties;

import cascading.pipe.joiner.Joiner;
import cascading.tuple.Fields;

/**
 * The CoGroup pipe allows for two or more tuple streams to join into a single stream via an optional {@link Joiner}.
 * <p/>
 * If followed by an assembly of {@link Every}s to execute one or more {@link cascading.operation.Aggregator}s,
 * they will be guaranteed to receive all values associated with a
 * unique grouping key. In the case of a MapReduce platform, this invokes a {@code Reduce} task to guarantee
 * all values are associated with a given unique grouping key.
 * <p/>
 * If no aggregations are to be performed, and one or more streams of data are small (may fit in reasonable memory),
 * see the {@link HashJoin} Pipe for partially non-blocking joins.
 * <p/>
 * For every incoming {@link Pipe} instance, a {@link Fields} instance must be specified that denotes the field names
 * or positions that should be co-grouped with the other given Pipe instances. If the incoming Pipe instances declare
 * one or more field with the same name, the declaredFields must be given to name all the outgoing Tuple stream fields
 * to overcome field name collisions. That is, if the first pipe has 4 fields, and the second pipe has 3 fields, 7 fields
 * total must be declared having unique field names, if any.
 * <p/>
 * {@code resultGroupFields} value is a convenience allowing the override of the resulting grouping field names. The
 * size of resultGroupFields must be equal to the total number of grouping keys fields. That is, if joining on two pipes
 * which are grouping on two keys, the resultGroupFields must be 4 fields, each field field name being unique, if any.
 * By default, the resultGroupKeys are retrieved from the declaredFields.
 * <p/>
 * By default CoGroup performs an inner join via the {@link cascading.pipe.joiner.InnerJoin}
 * {@link cascading.pipe.joiner.Joiner} class.
 * <p/>
 * Self joins can be achieved by using a constructor that takes a single Pipe and a numSelfJoins value. A value of
 * 1 for numSelfJoins will join the Pipe with itself once.
 * <p/>
 * The outgoing grouping Tuple stream is sorted by the natural order of the grouping fields. To control this order,
 * at least the first groupingFields value given should be an instance of {@link cascading.tuple.Fields} containing
 * {@link java.util.Comparator} instances for the appropriate fields.
 * This allows fine grained control of the sort grouping order.
 * <p/>
 * CoGrouping does not scale well when implemented over MapReduce. In Cascading there are two
 * ways to optimize CoGrouping.
 * <p/>
 * The first is to consider the order of the pipes handed to the CoGroup constructor.
 * <p/>
 * During co-grouping, for any given unique grouping key, all of the rightmost pipes will accumulate the current
 * grouping values into memory so they may be iterated across for every value in the left hand side pipe. During
 * the accumulation step, if the number of values exceeds the {@link cascading.tuple.collect.SpillableTupleList} threshold
 * value, those values will be spilled to disk so the accumulation may continue.
 * <p/>
 * See the {@link cascading.tuple.collect.TupleCollectionFactory} and {@link cascading.tuple.collect.TupleMapFactory} for a means
 * to use alternative spillable types.
 * <p/>
 * There is no accumulation for the left hand side pipe, only for those to the "right".
 * <p/>
 * Thus, for the pipe that has the largest number of values per unique key grouping, on average, it should be made the
 * "left hand side" pipe ({@code lhs}). And all remaining pipes should be the on the "right hand side" ({@code rhs}) to
 * prevent the likelihood of a spill and to reduce the blocking associated with accumulating the values. If using
 * the {@code Pipe[]} constructor, {@code Pipe[0]} is the left hand sided pipe.
 * <p/>
 * If spills are happening, consider increasing the spill threshold, see {@link cascading.tuple.collect.SpillableTupleList},
 * if more RAM is available. See the logs for hints on how much more these values can be increased, if any.
 * <p/>
 * Spills are intended to prevent {@link OutOfMemoryError}'s, so reducing the number of spills is important by
 * increasing the threshold, but memory errors aren't recoverable, so the correct balance will need to be found.
 * <p/>
 * To customize the spill values for a given CoGroup only, see {@link #getStepConfigDef()}.
 * <p/>
 * See the {@link cascading.tuple.Hasher} interface when a custom {@link java.util.Comparator} on the grouping keys is
 * being provided that makes two values with differing hashCode values equal. For example,
 * {@code new BigDecimal( 100.0D )} and {@code new Double 100.0D )} are equal using a custom Comparator, but
 * {@link Object#hashCode()} will be different, thus forcing each value into differing partitions.
 * <p/>
 * Currently "non-equi-joins" are not supported via the Hasher and Comparator interfaces. That is, joining one String
 * key with a lowercase value with another String key with an uppercase value using a "case insensitive" Comparator
 * will not have consistent results. The join will execute and be correct, but the actual values in the key columns may
 * be replaced with "equivalent" values from other streams.
 * <p/>
 * If the original key values must be retained, consider normalizing the keys with a Function and then joining on the
 * resulting field.
 *
 * @see cascading.pipe.joiner.InnerJoin
 * @see cascading.pipe.joiner.OuterJoin
 * @see cascading.pipe.joiner.LeftJoin
 * @see cascading.pipe.joiner.RightJoin
 * @see cascading.pipe.joiner.MixedJoin
 * @see cascading.tuple.Fields
 * @see cascading.tuple.collect.SpillableTupleList
 */
public class CoGroup extends Splice implements Group
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
  @ConstructorProperties({"lhs", "lhsGroupFields", "rhs", "rhsGroupFields", "declaredFields"})
  public CoGroup( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields )
    {
    super( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param lhs               of type Pipe
   * @param lhsGroupFields    of type Fields
   * @param rhs               of type Pipe
   * @param rhsGroupFields    of type Fields
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   */
  @ConstructorProperties({"lhs", "lhsGroupFields", "rhs", "rhsGroupFields", "declaredFields", "resultGroupFields"})
  public CoGroup( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Fields resultGroupFields )
    {
    super( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, resultGroupFields );
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
  @ConstructorProperties({"lhs", "lhsGroupFields", "rhs", "rhsGroupFields", "declaredFields", "joiner"})
  public CoGroup( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Joiner joiner )
    {
    super( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param lhs               of type Pipe
   * @param lhsGroupFields    of type Fields
   * @param rhs               of type Pipe
   * @param rhsGroupFields    of type Fields
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   * @param joiner            of type Joiner
   */
  @ConstructorProperties({"lhs", "lhsGroupFields", "rhs", "rhsGroupFields", "declaredFields", "resultGroupFields",
                          "joiner"})
  public CoGroup( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Fields resultGroupFields, Joiner joiner )
    {
    super( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, resultGroupFields, joiner );
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
  @ConstructorProperties({"lhs", "lhsGroupFields", "rhs", "rhsGroupFields", "joiner"})
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
  @ConstructorProperties({"lhs", "lhsGroupFields", "rhs", "rhsGroupFields"})
  public CoGroup( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields )
    {
    super( lhs, lhsGroupFields, rhs, rhsGroupFields );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param pipes of type Pipe...
   */
  @ConstructorProperties({"pipes"})
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
  @ConstructorProperties({"pipes", "groupFields"})
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
  @ConstructorProperties({"pipes", "groupFields", "declaredFields", "joiner"})
  public CoGroup( Pipe[] pipes, Fields[] groupFields, Fields declaredFields, Joiner joiner )
    {
    super( pipes, groupFields, declaredFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param pipes             of type Pipe[]
   * @param groupFields       of type Fields[]
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   * @param joiner            of type Joiner
   */
  @ConstructorProperties({"pipes", "groupFields", "declaredFields", "resultGroupFields", "joiner"})
  public CoGroup( Pipe[] pipes, Fields[] groupFields, Fields declaredFields, Fields resultGroupFields, Joiner joiner )
    {
    super( pipes, groupFields, declaredFields, resultGroupFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param groupName   of type String
   * @param pipes       of type Pipe[]
   * @param groupFields of type Fields[]
   */
  @ConstructorProperties({"groupName", "pipes", "groupFields"})
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
  @ConstructorProperties({"groupName", "pipes", "groupFields", "declaredFields"})
  public CoGroup( String groupName, Pipe[] pipes, Fields[] groupFields, Fields declaredFields )
    {
    super( groupName, pipes, groupFields, declaredFields );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param groupName         of type String
   * @param pipes             of type Pipe[]
   * @param groupFields       of type Fields[]
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   */
  @ConstructorProperties({"groupName", "pipes", "groupFields", "declaredFields", "resultGroupFields"})
  public CoGroup( String groupName, Pipe[] pipes, Fields[] groupFields, Fields declaredFields, Fields resultGroupFields )
    {
    super( groupName, pipes, groupFields, declaredFields, resultGroupFields );
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
  @ConstructorProperties({"groupName", "pipes", "groupFields", "declaredFields", "joiner"})
  public CoGroup( String groupName, Pipe[] pipes, Fields[] groupFields, Fields declaredFields, Joiner joiner )
    {
    super( groupName, pipes, groupFields, declaredFields, null, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param groupName         of type String
   * @param pipes             of type Pipe[]
   * @param groupFields       of type Fields[]
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   * @param joiner            of type CoGrouper
   */
  @ConstructorProperties({"groupName", "pipes", "groupFields", "declaredFields", "resultGroupFields", "joiner"})
  public CoGroup( String groupName, Pipe[] pipes, Fields[] groupFields, Fields declaredFields, Fields resultGroupFields, Joiner joiner )
    {
    super( groupName, pipes, groupFields, declaredFields, resultGroupFields, joiner );
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
  @ConstructorProperties({"groupName", "lhs", "lhsGroupFields", "rhs", "rhsGroupFields", "declaredFields"})
  public CoGroup( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields )
    {
    super( groupName, lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param groupName         of type String
   * @param lhs               of type Pipe
   * @param lhsGroupFields    of type Fields
   * @param rhs               of type Pipe
   * @param rhsGroupFields    of type Fields
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   */
  @ConstructorProperties({"groupName", "lhs", "lhsGroupFields", "rhs", "rhsGroupFields", "declaredFields",
                          "resultGroupFields"})
  public CoGroup( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Fields resultGroupFields )
    {
    super( groupName, lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, resultGroupFields );
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
  @ConstructorProperties({"groupName", "lhs", "lhsGroupFields", "rhs", "rhsGroupFields", "declaredFields", "joiner"})
  public CoGroup( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Joiner joiner )
    {
    super( groupName, lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param groupName         of type String
   * @param lhs               of type Pipe
   * @param lhsGroupFields    of type Fields
   * @param rhs               of type Pipe
   * @param rhsGroupFields    of type Fields
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   * @param joiner            of type Joiner
   */
  @ConstructorProperties({"groupName", "lhs", "lhsGroupFields", "rhs", "rhsGroupFields", "declaredFields",
                          "resultGroupFields", "joiner"})
  public CoGroup( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Fields resultGroupFields, Joiner joiner )
    {
    super( groupName, lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, resultGroupFields, joiner );
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
  @ConstructorProperties({"groupName", "lhs", "lhsGroupFields", "rhs", "rhsGroupFields", "joiner"})
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
  @ConstructorProperties({"groupName", "lhs", "lhsGroupFields", "rhs", "rhsGroupFields"})
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
  @ConstructorProperties({"groupName", "pipes"})
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
   * @param numSelfJoins   of type int
   * @param declaredFields of type Fields
   */
  @ConstructorProperties({"pipe", "groupFields", "numSelfJoins", "declaredFields"})
  public CoGroup( Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields )
    {
    super( pipe, groupFields, numSelfJoins, declaredFields );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param pipe              of type Pipe
   * @param groupFields       of type Fields
   * @param numSelfJoins      of type int
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   */
  @ConstructorProperties({"pipe", "groupFields", "numSelfJoins", "declaredFields", "resultGroupFields"})
  public CoGroup( Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Fields resultGroupFields )
    {
    super( pipe, groupFields, numSelfJoins, declaredFields, resultGroupFields );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance that performs numSelfJoins number of self joins on the
   * given {@link Pipe} instance.
   *
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param numSelfJoins   of type int
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  @ConstructorProperties({"pipe", "groupFields", "numSelfJoins", "declaredFields", "joiner"})
  public CoGroup( Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Joiner joiner )
    {
    super( pipe, groupFields, numSelfJoins, declaredFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param pipe              of type Pipe
   * @param groupFields       of type Fields
   * @param numSelfJoins      of type int
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   * @param joiner            of type Joiner
   */
  @ConstructorProperties({"pipe", "groupFields", "numSelfJoins", "declaredFields", "resultGroupFields", "joiner"})
  public CoGroup( Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Fields resultGroupFields, Joiner joiner )
    {
    super( pipe, groupFields, numSelfJoins, declaredFields, resultGroupFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance that performs numSelfJoins number of self joins on the
   * given {@link Pipe} instance.
   *
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param numSelfJoins of type int
   * @param joiner       of type CoGrouper
   */
  @ConstructorProperties({"pipe", "groupFields", "numSelfJoins", "joiner"})
  public CoGroup( Pipe pipe, Fields groupFields, int numSelfJoins, Joiner joiner )
    {
    super( pipe, groupFields, numSelfJoins, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance that performs numSelfJoins number of self joins on the
   * given {@link Pipe} instance.
   *
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param numSelfJoins of type int
   */
  @ConstructorProperties({"pipe", "groupFields", "numSelfJoins"})
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
   * @param numSelfJoins   of type int
   * @param declaredFields of type Fields
   */
  @ConstructorProperties({"groupName", "pipe", "groupFields", "numSelfJoins", "declaredFields"})
  public CoGroup( String groupName, Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields )
    {
    super( groupName, pipe, groupFields, numSelfJoins, declaredFields );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param groupName         of type String
   * @param pipe              of type Pipe
   * @param groupFields       of type Fields
   * @param numSelfJoins      of type int
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   */
  @ConstructorProperties({"groupName", "pipe", "groupFields", "numSelfJoins", "declaredFields", "resultGroupFields"})
  public CoGroup( String groupName, Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Fields resultGroupFields )
    {
    super( groupName, pipe, groupFields, numSelfJoins, declaredFields, resultGroupFields );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance that performs numSelfJoins number of self joins on the
   * given {@link Pipe} instance.
   *
   * @param groupName      of type String
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param numSelfJoins   of type int
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  @ConstructorProperties({"groupName", "pipe", "groupFields", "numSelfJoins", "declaredFields", "joiner"})
  public CoGroup( String groupName, Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Joiner joiner )
    {
    super( groupName, pipe, groupFields, numSelfJoins, declaredFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance.
   *
   * @param groupName         of type String
   * @param pipe              of type Pipe
   * @param groupFields       of type Fields
   * @param numSelfJoins      of type int
   * @param declaredFields    of type Fields
   * @param resultGroupFields of type Fields
   * @param joiner            of type Joiner
   */
  @ConstructorProperties({"groupName", "pipe", "groupFields", "numSelfJoins", "declaredFields", "resultGroupFields",
                          "joiner"})
  public CoGroup( String groupName, Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Fields resultGroupFields, Joiner joiner )
    {
    super( groupName, pipe, groupFields, numSelfJoins, declaredFields, resultGroupFields, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance that performs numSelfJoins number of self joins on the
   * given {@link Pipe} instance.
   *
   * @param groupName    of type String
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param numSelfJoins of type int
   * @param joiner       of type CoGrouper
   */
  @ConstructorProperties({"groupName", "pipe", "groupFields", "numSelfJoins", "joiner"})
  public CoGroup( String groupName, Pipe pipe, Fields groupFields, int numSelfJoins, Joiner joiner )
    {
    super( groupName, pipe, groupFields, numSelfJoins, joiner );
    }

  /**
   * Constructor CoGroup creates a new CoGroup instance that performs numSelfJoins number of self joins on the
   * given {@link Pipe} instance.
   *
   * @param groupName    of type String
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param numSelfJoins of type int
   */
  @ConstructorProperties({"groupName", "pipe", "groupFields", "numSelfJoins"})
  public CoGroup( String groupName, Pipe pipe, Fields groupFields, int numSelfJoins )
    {
    super( groupName, pipe, groupFields, numSelfJoins );
    }
  }
