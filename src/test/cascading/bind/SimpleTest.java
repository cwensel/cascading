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

import cascading.CascadingTestCase;
import cascading.cascade.Cascade;
import cascading.flow.Flow;
import cascading.tap.Tap;
import cascading.test.PlatformTest;

/**
 *
 */
@PlatformTest(platforms = {"none"})
public class SimpleTest extends CascadingTestCase
  {
  public void testSchema()
    {
    PersonSchema schema = new PersonSchema();

    assertNotNull( schema.getTapFor( new ConversionResource( "foo", Protocol.HDFS, Format.TSV ) ) );
    assertNotNull( schema.getTapFor( new ConversionResource( "foo", Protocol.HTTP, Format.JSON ) ) );
    }

  public void testFlowFactory()
    {
    CSVToTSVFactory factory = new CSVToTSVFactory( "convert", new PersonSchema() );

    factory.setSource( Protocol.HDFS, "some/path" );
    factory.setSink( Protocol.HTTP, "http://some/place" );

    Flow flow = factory.create();

    assertEquals( 1, flow.getSourcesCollection().size() );
    assertEquals( 1, flow.getSinksCollection().size() );

    assertEquals( "some/path", flow.getSource( "convert" ).getIdentifier() );
    assertEquals( "http://some/place", flow.getSink( "convert" ).getIdentifier() );
    }

  public void testCascadeFactory()
    {
    // these factories read from the same location, but pretend they do something different
    CSVToTSVFactory factory1 = new CSVToTSVFactory( "convert1", new PersonSchema() );
    factory1.setSource( Protocol.HDFS, "some/remote/path" );
    factory1.setSink( Protocol.HDFS, "some/place/first" );

    CSVToTSVFactory factory2 = new CSVToTSVFactory( "convert2", new PersonSchema() );
    factory2.setSource( Protocol.HDFS, "some/remote/path" );
    factory2.setSink( Protocol.HDFS, "some/place/second" );

    CSVToTSVFactory factory3 = new CSVToTSVFactory( "convert3", new PersonSchema() );
    factory3.setSource( Protocol.HDFS, "some/remote/path" );
    factory3.setSink( Protocol.HDFS, "some/place/third" );

    // will insert a copy flow if more than one other flow-factory is reading
    // from the same remote source, then update the dependent factories
    // with the new location
    TestCacheFactory resourcesFactory = new TestCacheFactory( "copy" );
    resourcesFactory.setSource( "some/remote/path" );
    resourcesFactory.setSink( "some/local/path" );

    resourcesFactory.addProcessFactory( factory1 );
    resourcesFactory.addProcessFactory( factory2 );
    resourcesFactory.addProcessFactory( factory3 );

    Cascade cascade = resourcesFactory.create();

    assertTrue( cascade.getFlows().get( 0 ).getName(), cascade.getFlows().get( 0 ).getName().startsWith( "copy" ) );

    for( Flow flow : cascade.getFlows() )
      {
      if( flow.getName().startsWith( "convert" ) )
        assertEquals( "some/local/path", ( (Tap) flow.getSourcesCollection().iterator().next() ).getIdentifier() );
      }
    }
  }
