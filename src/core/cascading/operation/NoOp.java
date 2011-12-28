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

package cascading.operation;

import cascading.flow.FlowProcess;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Class NoOp does nothing. It ignores all arguments and returns an empty {@link Tuple}, {@link Tuple#NULL}.
 * <p/>
 * Use with {@link Fields#SWAP} to retain unknown fields, or use the {@link cascading.pipe.assembly.Discard}
 * sub-assembly.
 *
 * @see Insert
 * @see cascading.pipe.assembly.Discard
 */
public class NoOp extends BaseOperation implements Function
  {
  /** Constructor NoOp creates a new NoOp instance that will ignore the argument values and return no output. */
  public NoOp()
    {
    super( ANY, Fields.NONE );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
    functionCall.getOutputCollector().add( Tuple.NULL );
    }
  }
