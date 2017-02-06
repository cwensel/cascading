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

  public void start( FlowProcess flowProcess, AggregatorCall aggregatorCall )
    {
    if( fail == 0 )
      throw new RuntimeException( "failed" );
    }

  public void aggregate( FlowProcess flowProcess, AggregatorCall aggregatorCall )
    {
    if( fail == 1 )
      throw new RuntimeException( "failed" );
    }

  public void complete( FlowProcess flowProcess, AggregatorCall aggregatorCall )
    {
    if( fail == 2 )
      throw new RuntimeException( "failed" );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof TestFailAggregator ) )
      return false;
    if( !super.equals( object ) )
      return false;

    TestFailAggregator that = (TestFailAggregator) object;

    if( fail != that.fail )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + fail;
    return result;
    }
  }