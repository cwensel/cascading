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

package cascading;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/** @version $Id: //depot/calku/cascading/src/test/cascading/TestAggregator.java#2 $ */
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
    for( int i = 0; i < duplicates; i++ )
      {
      for( Tuple tuple : value )
        aggregatorCall.getOutputCollector().add( tuple );
      }
    }
  }