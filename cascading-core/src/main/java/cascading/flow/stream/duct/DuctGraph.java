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

package cascading.flow.stream.duct;

import org.jgrapht.EdgeFactory;
import org.jgrapht.graph.SimpleDirectedGraph;

/**
 *
 */
public class DuctGraph extends SimpleDirectedGraph<Duct, DuctGraph.Ordinal>
  {
  private static class DuctOrdinalEdgeFactory implements EdgeFactory<Duct, Ordinal>
    {
    int count = 0;

    @Override
    public DuctGraph.Ordinal createEdge( Duct lhs, Duct rhs )
      {
      return makeOrdinal( 0 );
      }

    public DuctGraph.Ordinal makeOrdinal( int ordinal )
      {
      return new DuctGraph.Ordinal( count++, ordinal );
      }
    }

  public static class Ordinal
    {
    int count;
    int ordinal;

    public Ordinal( int count, int ordinal )
      {
      this.count = count;
      this.ordinal = ordinal;
      }

    public int getOrdinal()
      {
      return ordinal;
      }

    @Override
    public boolean equals( Object object )
      {
      if( this == object )
        return true;

      Ordinal ordinal = (Ordinal) object;

      if( count != ordinal.count )
        return false;

      return true;
      }

    @Override
    public int hashCode()
      {
      return count;
      }
    }

  public DuctGraph()
    {
    super( new DuctOrdinalEdgeFactory() );
    }

  public synchronized DuctGraph.Ordinal makeOrdinal( int ordinal )
    {
    return ( (DuctOrdinalEdgeFactory) getEdgeFactory() ).makeOrdinal( ordinal );
    }
  }
