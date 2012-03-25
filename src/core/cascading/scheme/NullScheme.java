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

package cascading.scheme;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 *
 */
public class NullScheme<Process extends FlowProcess, Config, Input, Output, SourceContext, SinkContext> extends Scheme<Process, Config, Input, Output, SourceContext, SinkContext>
  {
  public NullScheme()
    {
    }

  public NullScheme( Fields sourceFields, Fields sinkFields )
    {
    super( sourceFields, sinkFields );
    }

  public void sourceConfInit( Process flowProcess, Tap<Process, Config, Input, Output> tap, Config conf )
    {
    }

  public void sinkConfInit( Process flowProcess, Tap<Process, Config, Input, Output> tap, Config conf )
    {
    }

  public boolean source( Process flowProcess, SourceCall<SourceContext, Input> sourceCall ) throws IOException
    {
    throw new UnsupportedOperationException( "sourcing is not supported in the scheme" );
    }

  public void sink( Process flowProcess, SinkCall<SinkContext, Output> sinkCall ) throws IOException
    {
    throw new UnsupportedOperationException( "sinking is not supported in the scheme" );
    }

  @Override
  public String toString()
    {
    return getClass().getSimpleName();
    }
  }
