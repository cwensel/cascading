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

package cascading.scheme.local;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;

import cascading.flow.local.LocalFlowProcess;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;

/**
 * Class LocalScheme is the abstract base class for all local mode {@link Scheme} classes. It is offered as a
 * convenience for those implementing custom Scheme types.
 */
public abstract class LocalScheme<Input, Output, SourceContext, SinkContext> extends Scheme<LocalFlowProcess, Properties, Input, Output, SourceContext, SinkContext>
  {
  protected LocalScheme()
    {
    }

  protected LocalScheme( Fields sourceFields )
    {
    super( sourceFields );
    }

  protected LocalScheme( Fields sourceFields, Fields sinkFields )
    {
    super( sourceFields, sinkFields );
    }

  public abstract Input createInput( FileInputStream inputStream );

  public abstract Output createOutput( FileOutputStream outputStream );
  }
