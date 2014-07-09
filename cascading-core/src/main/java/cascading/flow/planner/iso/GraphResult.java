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

package cascading.flow.planner.iso;

import java.io.File;

import cascading.flow.planner.graph.ElementGraph;

/**
 *
 */
public abstract class GraphResult<R extends ElementGraph>
  {
  public abstract ElementGraph getBeginGraph();

  public abstract String getRuleName();

  public abstract R getEndGraph();

  public abstract void writeDOTs( String path );

  protected void writeEndGraph( String path, int count )
    {
    if( getEndGraph() != null )
      {
      String name = getEndGraph().getClass().getSimpleName();
      getEndGraph().writeDOT( new File( path, makeFileName( count, name, "end" ) ).toString() );
      }
    }

  protected int writeBeginGraph( String path, int count )
    {
    if( getBeginGraph() != null )
      {
      String name = getBeginGraph().getClass().getSimpleName();
      getBeginGraph().writeDOT( new File( path, makeFileName( count++, name, "begin" ) ).toString() );
      }

    return count;
    }

  protected String makeFileName( int ordinal, String name, String state )
    {
    return String.format( "%04d-%s-%s.dot", ordinal, name, state );
    }
  }