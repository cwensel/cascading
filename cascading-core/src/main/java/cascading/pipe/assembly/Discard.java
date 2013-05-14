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

package cascading.pipe.assembly;

import java.beans.ConstructorProperties;

import cascading.operation.NoOp;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/**
 * Class Discard is a {@link cascading.pipe.SubAssembly} that will retain all incoming fields except those given on the constructor.
 * <p/>
 * Use this pipe to narrow a stream of tuples, removing unused data.
 *
 * @see Retain
 */
public class Discard extends SubAssembly
  {
  @ConstructorProperties({"previous", "discardFields"})
  public Discard( Pipe previous, Fields discardFields )
    {
    super( previous );

    setTails( new Each( previous, discardFields, new NoOp(), Fields.SWAP ) );
    }
  }
