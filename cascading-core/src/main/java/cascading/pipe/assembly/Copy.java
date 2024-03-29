/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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
 * Class Copy is a {@link SubAssembly} that will copy the values of the fromFields to the toFields.
 * <p>
 * If the type information is provided on the toFields, the values will be coerced into the new canonical type.
 */
public class Copy extends SubAssembly
  {
  /**
   * Copy the fromFields in the current Tuple to the given toFields.
   * <pre>
   * incoming: {"first", "middle", "last"} -> from:{"middle"} to:{"initial"} -> outgoing:{"first", "last", "middle", "initial"}
   * </pre>
   *
   * @param previous   of type Pipe
   * @param fromFields of type Fields
   * @param toFields   of type Fields
   */
  @ConstructorProperties({"previous", "fromFields", "toFields"})
  public Copy( Pipe previous, Fields fromFields, Fields toFields )
    {
    super( previous );

    if( fromFields == null )
      throw new IllegalArgumentException( "fromFields may not be null" );

    if( toFields == null )
      throw new IllegalArgumentException( "toFields may not be null" );

    if( fromFields.isDefined() && fromFields.size() != toFields.size() )
      throw new IllegalArgumentException( "fields arguments must be same size, from: " + fromFields.printVerbose() + " to: " + toFields.printVerbose() );

    if( !toFields.isDefined() )
      throw new IllegalArgumentException( "toFields must define field names: " + toFields.printVerbose() );

    setTails( new Each( previous, fromFields, new Identity( toFields ), Fields.ALL ) );
    }
  }