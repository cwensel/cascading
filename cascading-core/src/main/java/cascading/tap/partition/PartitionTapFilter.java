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

package cascading.tap.partition;

import java.io.Serializable;

import cascading.flow.FlowProcess;
import cascading.operation.ConcreteCall;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

class PartitionTapFilter implements Serializable
  {
  private final Fields argumentFields;
  private final Filter filter;
  private transient FilterCall<?> filterCall;

  PartitionTapFilter( Fields argumentSelector, Filter filter )
    {
    this.argumentFields = argumentSelector;
    this.filter = filter;
    }

  protected FilterCall<?> getFilterCall()
    {
    if( filterCall == null )
      filterCall = new ConcreteCall( argumentFields );

    return filterCall;
    }

  private FilterCall<?> getFilterCallWith( TupleEntry arguments )
    {
    FilterCall<?> filterCall = getFilterCall();

    ( (ConcreteCall) filterCall ).setArguments( arguments );

    return filterCall;
    }

  void prepare( FlowProcess flowProcess )
    {
    filter.prepare( flowProcess, getFilterCall() );
    }

  boolean isRemove( FlowProcess flowProcess, TupleEntry partitionEntry )
    {
    return filter.isRemove( flowProcess, getFilterCallWith( partitionEntry.selectEntry( argumentFields ) ) );
    }

  void cleanup( FlowProcess flowProcess )
    {
    filter.cleanup( flowProcess, getFilterCall() );
    }
  }
