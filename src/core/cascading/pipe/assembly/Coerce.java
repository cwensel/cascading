/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.pipe.assembly;

import java.beans.ConstructorProperties;

import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/**
 * Class Coerce is a {@link SubAssembly} that will coerce all incoming {@link cascading.tuple.Tuple} values to
 * the given types.
 * <p/>
 * Coerce encapsulates the {@link Identity} function.
 *
 * @see cascading.pipe.SubAssembly
 * @see cascading.operation.Identity
 */
public class Coerce extends SubAssembly
  {
  /**
   * Constructor Coerce creates a new Coerce instance that will coerce all input Tuple values.
   *
   * @param previous of type Pipe
   * @param types    of type Class...
   */
  @ConstructorProperties({"previous", "types"})
  public Coerce( Pipe previous, Class... types )
    {
    setTails( new Each( previous, new Identity( types ) ) );
    }

  /**
   * Constructor Coerce creates a new Coerce instance that will only coerce the given coerceFields Tuple values.
   * <p/>
   * Note the resulting output Tuple will contain all the original incoming Fields.
   *
   * @param previous     of type Pipe
   * @param coerceFields of type Fields
   * @param types        of type Class...
   */
  @ConstructorProperties({"previous", "coerceFields", "types"})
  public Coerce( Pipe previous, Fields coerceFields, Class... types )
    {
    setTails( new Each( previous, coerceFields, new Identity( types ), Fields.REPLACE ) );
    }
  }
