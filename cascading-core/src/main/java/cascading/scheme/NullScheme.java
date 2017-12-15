/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.scheme;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Class NullScheme is a {@link Scheme} that neither reads or writes any values.
 * <p>
 * It is typically used as a placeholder where a Scheme instance is needed but generally ignored.
 */
public class NullScheme<Config, Input, Output, SourceContext, SinkContext> extends Scheme<Config, Input, Output, SourceContext, SinkContext>
  {
  public NullScheme()
    {
    }

  public NullScheme( Fields sourceFields, Fields sinkFields )
    {
    super( sourceFields, sinkFields );
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Config> flowProcess, Tap<Config, Input, Output> tap, Config conf )
    {
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends Config> flowProcess, Tap<Config, Input, Output> tap, Config conf )
    {
    }

  @Override
  public boolean source( FlowProcess<? extends Config> flowProcess, SourceCall<SourceContext, Input> sourceCall ) throws IOException
    {
    return false;
    }

  @Override
  public void sink( FlowProcess<? extends Config> flowProcess, SinkCall<SinkContext, Output> sinkCall ) throws IOException
    {

    }

  @Override
  public String toString()
    {
    return getClass().getSimpleName();
    }
  }
