/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stream;

import cascading.flow.FlowProcess;
import cascading.pipe.Each;
import cascading.pipe.Operator;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 *
 */
public abstract class EachStage extends OperatorStage<TupleEntry> implements Mapping
  {
  final Each each;

  public EachStage( FlowProcess flowProcess, Each each )
    {
    super( flowProcess, each );
    this.each = each;
    }

  @Override
  public Operator getOperator()
    {
    return each;
    }

  @Override
  protected Fields getOutgoingSelector()
    {
    return outgoingScopes.get( 0 ).getOutValuesSelector();
    }
  }
