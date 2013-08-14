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

package cascading.operation.aggregator;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Class First is an {@link Aggregator} that returns the first {@link Tuple} encountered in a grouping.
 * <p/>
 * By default, it returns the first Tuple of {@link Fields#ARGS} found.
 * <p/>
 * If {@code firstN} is given, Tuples with each of the first N number of Tuples encountered are returned. That is,
 * this Aggregator will return at maximum N tuples per grouping.
 * <p/>
 * Be sure to set the {@link cascading.pipe.GroupBy} {@code sortFields} to control which Tuples are seen first.
 */
public class First extends ExtentBase
  {
  private final int firstN;

  /** Selects and returns the first argument Tuple encountered. */
  public First()
    {
    super( Fields.ARGS );

    this.firstN = 1;
    }

  /**
   * Selects and returns the first N argument Tuples encountered.
   *
   * @param firstN of type int
   */
  @ConstructorProperties({"firstN"})
  public First( int firstN )
    {
    super( Fields.ARGS );

    this.firstN = firstN;
    }

  /**
   * Selects and returns the first argument Tuple encountered.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public First( Fields fieldDeclaration )
    {
    super( fieldDeclaration.size(), fieldDeclaration );

    this.firstN = 1;
    }

  /**
   * Selects and returns the first N argument Tuples encountered.
   *
   * @param fieldDeclaration of type Fields
   * @param firstN           of type int
   */
  @ConstructorProperties({"fieldDeclaration", "firstN"})
  public First( Fields fieldDeclaration, int firstN )
    {
    super( fieldDeclaration.size(), fieldDeclaration );

    this.firstN = firstN;
    }

  /**
   * Selects and returns the first argument Tuple encountered, unless the Tuple
   * is a member of the set ignoreTuples.
   *
   * @param fieldDeclaration of type Fields
   * @param ignoreTuples     of type Tuple...
   */
  @ConstructorProperties({"fieldDeclaration", "ignoreTuples"})
  public First( Fields fieldDeclaration, Tuple... ignoreTuples )
    {
    super( fieldDeclaration, ignoreTuples );

    this.firstN = 1;
    }

  public int getFirstN()
    {
    return firstN;
    }

  protected void performOperation( Tuple[] context, TupleEntry entry )
    {
    if( context[ 0 ] == null )
      context[ 0 ] = new Tuple();

    if( context[ 0 ].size() < firstN )
      context[ 0 ].add( entry.getTupleCopy() );
    }

  @Override
  public void complete( FlowProcess flowProcess, AggregatorCall<Tuple[]> aggregatorCall )
    {
    Tuple context = aggregatorCall.getContext()[ 0 ];

    if( context == null )
      return;

    for( Object tuple : context )
      aggregatorCall.getOutputCollector().add( (Tuple) tuple );
    }
  }
