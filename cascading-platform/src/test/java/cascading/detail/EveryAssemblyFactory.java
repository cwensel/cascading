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

package cascading.detail;

import cascading.TestAggregator;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class EveryAssemblyFactory extends AssemblyFactory
  {
  public Pipe createAssembly( Pipe pipe, Fields argFields, Fields declFields, String fieldValue, Fields selectFields )
    {
    pipe = new GroupBy( pipe, Fields.ALL );

    return new Every( pipe, argFields, new TestAggregator( declFields, new Tuple( fieldValue ) ), selectFields );
    }
  }