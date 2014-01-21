/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner.iso.expression;

import cascading.flow.FlowElement;
import cascading.operation.Operation;
import cascading.pipe.Operator;

/**
 *
 */
public class OperationExpression extends TypeExpression<Operation>
  {
  public OperationExpression( Capture capture, boolean exact, Class<? extends Operation> type )
    {
    super( capture, exact, type );
    }

  public OperationExpression( Capture capture, Class<? extends Operation> type )
    {
    super( capture, type );
    }

  public OperationExpression( boolean exact, Class<? extends Operation> type )
    {
    super( exact, type );
    }

  public OperationExpression( Class<? extends Operation> type )
    {
    super( type );
    }

  @Override
  protected Class<? extends Operation> getType( FlowElement flowElement )
    {
    if( !( flowElement instanceof Operator ) )
      return null;

    return ( (Operator) flowElement ).getOperation().getClass();
    }
  }
