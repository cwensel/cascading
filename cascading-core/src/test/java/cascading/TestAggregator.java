/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class TestAggregator extends BaseOperation implements Aggregator
  {
  private static final long serialVersionUID = 1L;
  private Tuple[] value;
  private int duplicates = 1;
  private Fields groupFields;

  /**
   * Constructor
   *
   * @param fields the fields to operate on
   * @param value
   */
  public TestAggregator( Fields fields, Tuple... value )
    {
    super( fields );
    this.value = value;
    }

  public TestAggregator( Fields fields, Fields groupFields, Tuple... value )
    {
    super( fields );
    this.groupFields = groupFields;
    this.value = value;
    }

  /**
   * Constructor TestAggregator creates a new TestAggregator instance.
   *
   * @param fieldDeclaration of type Fields
   * @param value            of type Tuple
   * @param duplicates       of type int
   */
  public TestAggregator( Fields fieldDeclaration, Tuple value, int duplicates )
    {
    super( fieldDeclaration );
    this.value = new Tuple[]{value};
    this.duplicates = duplicates;
    }

  public TestAggregator( Fields fieldDeclaration, Fields groupFields, Tuple value, int duplicates )
    {
    super( fieldDeclaration );
    this.groupFields = groupFields;
    this.value = new Tuple[]{value};
    this.duplicates = duplicates;
    }

  public void start( FlowProcess flowProcess, AggregatorCall aggregatorCall )
    {
    if( groupFields == null )
      return;

    if( !groupFields.equals( aggregatorCall.getGroup().getFields() ) )
      throw new RuntimeException( "fields do not match: " + groupFields.print() + " != " + aggregatorCall.getGroup().getFields().print() );
    }

  public void aggregate( FlowProcess flowProcess, AggregatorCall aggregatorCall )
    {
    }

  public void complete( FlowProcess flowProcess, AggregatorCall aggregatorCall )
    {
    TupleEntry result;

    if( aggregatorCall.getDeclaredFields().isUnknown() )
      result = new TupleEntry( Fields.size( value[ 0 ].size() ), Tuple.size( value[ 0 ].size() ) );
    else
      result = new TupleEntry( aggregatorCall.getDeclaredFields(), Tuple.size( aggregatorCall.getDeclaredFields().size() ) );

    for( int i = 0; i < duplicates; i++ )
      {
      for( Tuple tuple : value )
        {
        try
          {
          result.setCanonicalTuple( tuple );
          }
        catch( Exception exception )
          {
          // value is a string, but the the test declared a numeric type
          result.setCanonicalTuple( Tuple.size( value[ 0 ].size(), -99 ) );
          }

        aggregatorCall.getOutputCollector().add( result );
        }
      }
    }
  }