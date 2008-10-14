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

package cascading.operation.aggregator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import cascading.flow.FlowSession;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/** Class ExtentBase is the base class for First and Last. */
public abstract class ExtentBase extends BaseOperation implements Aggregator<Tuple[]>
  {
  /** Field ignoreTuples */
  private final Collection<Tuple> ignoreTuples;

  protected ExtentBase( Fields fieldDeclaration )
    {
    super( fieldDeclaration );
    this.ignoreTuples = null;
    }

  protected ExtentBase( int numArgs, Fields fieldDeclaration )
    {
    super( numArgs, fieldDeclaration );
    ignoreTuples = null;
    }

  protected ExtentBase( Fields fieldDeclaration, Tuple... ignoreTuples )
    {
    super( fieldDeclaration );
    this.ignoreTuples = new HashSet<Tuple>();
    Collections.addAll( this.ignoreTuples, ignoreTuples );
    }

  @SuppressWarnings("unchecked")
  public void start( FlowSession flowSession, AggregatorCall<Tuple[]> aggregatorCall )
    {
    if( aggregatorCall.getContext() == null )
      aggregatorCall.setContext( new Tuple[1] );
    else
      aggregatorCall.getContext()[ 0 ] = null;
    }

  public void aggregate( FlowSession flowSession, AggregatorCall<Tuple[]> aggregatorCall )
    {
    if( ignoreTuples != null && ignoreTuples.contains( aggregatorCall.getArguments().getTuple() ) )
      return;

    performOperation( aggregatorCall.getContext(), aggregatorCall.getArguments() );
    }

  protected abstract void performOperation( Tuple[] context, TupleEntry entry );

  public void complete( FlowSession flowSession, AggregatorCall<Tuple[]> aggregatorCall )
    {
    if( aggregatorCall.getContext()[ 0 ] != null )
      aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
    }

  protected Tuple getResult( AggregatorCall<Tuple[]> aggregatorCall )
    {
    return aggregatorCall.getContext()[ 0 ];
    }
  }
