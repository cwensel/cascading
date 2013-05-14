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

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/**
 * Class Retain is a {@link SubAssembly} that will discard all incoming fields except those given on the constructor.
 * <p/>
 * Use this pipe to narrow a stream of tuples, removing unused data.
 *
 * @see Discard
 */
public class Retain extends SubAssembly
  {
  @ConstructorProperties({"previous", "retainFields"})
  public Retain( Pipe previous, Fields retainFields )
    {
    super( previous );
    setTails( new Each( previous, retainFields, new Identity(), Fields.RESULTS ) );
    }
  }
