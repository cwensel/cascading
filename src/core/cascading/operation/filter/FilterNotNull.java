/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.filter;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;

/**
 * Class FilterNotNull verifies that every value in the argument values {@link cascading.tuple.Tuple}
 * is a null value. If a non-null value is encountered, the current Tuple will be filtered out.
 *
 * @see FilterNull
 */
public class FilterNotNull extends BaseOperation implements Filter
  {
  public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
    {
    for( Object value : filterCall.getArguments().getTuple() )
      {
      if( value != null )
        return true;
      }

    return false;
    }
  }
