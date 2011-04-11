/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.bind;

import java.util.Properties;

import cascading.bind.factory.FlowFactory;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;

/**
 * A mock FlowFactory that creates a working Flow for conversion of data between
 * two formats and across multiple protocols.
 */
public class TestCopyFactory extends FlowFactory
  {
  private String name;

  public TestCopyFactory( String name )
    {
    this( null, name );
    }

  public TestCopyFactory( Properties properties, String name )
    {
    super( properties );
    this.name = name;

    setSourceSchema( name, new CopySchema() );
    setSinkSchema( name, new CopySchema() );
    }

  public void addSourceResource( TapResource resource )
    {
    addSourceResource( name, resource );
    }

  public void addSinkResource( TapResource resource )
    {
    addSinkResource( name, resource );
    }

  @Override
  protected FlowConnector getFlowConnector()
    {
    return new HadoopFlowConnector( getProperties() );
    }

  @Override
  public Flow create()
    {
    Pipe pipe = new Pipe( name ); // this forces pipe-lining between the source and sink

    return createFlowFrom( name, pipe );
    }
  }
