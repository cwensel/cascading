/*
 * Copyright (c) 2016-2017 Chris K Wensel. All Rights Reserved.
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

package cascading.nested.json;

import java.util.List;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.nested.core.CopySpec;
import cascading.nested.json.transform.JSONPrimitiveTransforms;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;

import static data.InputData.inputFileJSON;

/**
 *
 */
public class JSONNestedPlatformTest extends PlatformTestCase
  {
  public JSONNestedPlatformTest()
    {
    }

  @Test
  public void testSimpleGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileJSON );

    Tap source = getPlatform().getJSONFile( new Fields( "json" ), inputFileJSON );

    Pipe pipe = new Pipe( "test" );

    CopySpec copySpec = new CopySpec()
      .include( "/_id/$oid", "/borrower", "/project_name", "/approvalfy" )
      .transform( "/approvalfy", JSONPrimitiveTransforms.TO_STRING );

    pipe = new Each( pipe, new Fields( "json" ), new JSONCopyAsFunction( new Fields( "result" ), copySpec ), Fields.RESULTS );

//    pipe = new Each( pipe, new Debug( true ) );

    pipe = new Each( pipe, new Fields( "result" ), new JSONGetFunction( new Fields( "year", int.class ), "/approvalfy" ), Fields.ALL );

    pipe = new GroupBy( pipe, new Fields( "year" ) );

    Tap sink = getPlatform().getJSONFile( new Fields( "result" ), getOutputPath(), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 20 );

    List<Tuple> results = asList( flow, sink );

    assertEquals( 20, results.size() );

    boolean allMatch = results.stream()
      .map( tuple -> tuple.getObject( 0 ) )
      .allMatch( node -> node instanceof JsonNode );

    assertTrue( allMatch );
    }
  }
