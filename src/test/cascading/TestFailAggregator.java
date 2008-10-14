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

import cascading.flow.FlowSession;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;

/** @version $Id: //depot/calku/cascading/src/test/cascading/TestAggregator.java#2 $ */
public class TestFailAggregator extends BaseOperation implements Aggregator
  {
  private static final long serialVersionUID = 1L;
  private final int fail;

  /**
   * Constructor
   *
   * @param fields the fields to operate on
   */
  public TestFailAggregator( Fields fields, int fail )
    {
    super( fields );
    this.fail = fail;
    }

  public void start( FlowSession flowSession, AggregatorCall aggregatorCall )
    {
    if( fail == 0 )
      throw new RuntimeException( "failed" );
    }

  public void aggregate( FlowSession flowSession, AggregatorCall aggregatorCall )
    {
    if( fail == 1 )
      throw new RuntimeException( "failed" );
    }

  public void complete( FlowSession flowSession, AggregatorCall aggregatorCall )
    {
    if( fail == 2 )
      throw new RuntimeException( "failed" );
    }
  }