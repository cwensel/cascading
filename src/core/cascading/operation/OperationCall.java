/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation;

import cascading.tuple.Fields;

/**
 *
 */
public interface OperationCall<C>
  {
  /**
   * Returns the user set context object, C.
   *
   * @return user defined object
   */
  C getContext();

  /**
   * Sets the user defined 'context' object.
   *
   * @param context user defined object
   */
  void setContext( C context );

  /**
   * Returns the {@link Fields} of the expected arguments {@link cascading.tuple.TupleEntry}.
   *
   * @return the argumentFields (type Fields) of this OperationCall object.
   */
  Fields getArgumentFields();
  }