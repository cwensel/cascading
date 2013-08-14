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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/** Class ExtentBase is the base class for First and Last. */
public abstract class ExtentBase extends BaseOperation<Tuple[]> implements Aggregator<Tuple[]>
  {
  /** Field ignoreTuples */
  private final Collection<Tuple> ignoreTuples;

  @ConstructorProperties({"fieldDeclaration"})
  protected ExtentBase( Fields fieldDeclaration )
    {
    super( fieldDeclaration );
    this.ignoreTuples = null;
    }

  @ConstructorProperties({"numArgs", "fieldDeclaration"})
  protected ExtentBase( int numArgs, Fields fieldDeclaration )
    {
    super( numArgs, fieldDeclaration );
    ignoreTuples = null;
    }

  @ConstructorProperties({"fieldDeclaration", "ignoreTuples"})
  protected ExtentBase( Fields fieldDeclaration, Tuple... ignoreTuples )
    {
    super( fieldDeclaration );
    this.ignoreTuples = new HashSet<Tuple>();
    Collections.addAll( this.ignoreTuples, ignoreTuples );
    }

  public Collection<Tuple> getIgnoreTuples()
    {
    return Collections.unmodifiableCollection( ignoreTuples );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Tuple[]> operationCall )
    {
    operationCall.setContext( new Tuple[ 1 ] );
    }

  @Override
  public void start( FlowProcess flowProcess, AggregatorCall<Tuple[]> aggregatorCall )
    {
    aggregatorCall.getContext()[ 0 ] = null;
    }

  @Override
  public void aggregate( FlowProcess flowProcess, AggregatorCall<Tuple[]> aggregatorCall )
    {
    if( ignoreTuples != null && ignoreTuples.contains( aggregatorCall.getArguments().getTuple() ) )
      return;

    performOperation( aggregatorCall.getContext(), aggregatorCall.getArguments() );
    }

  protected abstract void performOperation( Tuple[] context, TupleEntry entry );

  @Override
  public void complete( FlowProcess flowProcess, AggregatorCall<Tuple[]> aggregatorCall )
    {
    if( aggregatorCall.getContext()[ 0 ] != null )
      aggregatorCall.getOutputCollector().add( getResult( aggregatorCall ) );
    }

  protected Tuple getResult( AggregatorCall<Tuple[]> aggregatorCall )
    {
    return aggregatorCall.getContext()[ 0 ];
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof ExtentBase ) )
      return false;
    if( !super.equals( object ) )
      return false;

    ExtentBase that = (ExtentBase) object;

    if( ignoreTuples != null ? !ignoreTuples.equals( that.ignoreTuples ) : that.ignoreTuples != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( ignoreTuples != null ? ignoreTuples.hashCode() : 0 );
    return result;
    }
  }
