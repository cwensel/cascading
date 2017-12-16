/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2016 Xplenty, Inc. All Rights Reserved.
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

package cascading.flow;

/**
 * Class FlowTapException is thrown during flow execution to denote an issue with a source or sink
 * {@link cascading.tap.Tap}.
 */
public class FlowTapException extends FlowException
  {
  public FlowTapException()
    {
    }

  public FlowTapException( String message )
    {
    super( message );
    }

  public FlowTapException( String message, Throwable throwable )
    {
    super( message, throwable );
    }

  public FlowTapException( String flowName, String message, Throwable throwable )
    {
    super( flowName, message, throwable );
    }

  public FlowTapException( String flowName, String message )
    {
    super( flowName, message );
    }

  public FlowTapException( Throwable throwable )
    {
    super( throwable );
    }
  }
