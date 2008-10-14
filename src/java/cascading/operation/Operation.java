/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.FlowSession;
import cascading.tuple.Fields;

/**
 * Interface Operation is the base interface for all functions applied to {@link cascading.tuple.Tuple} streams.
 * <p/>
 * Specificlly {@link Function}, {@link Filter}, {@link Aggregator}, and {@link Assertion}.
 * <p/>
 * Use {@link BaseOperation} for a convenient way to create new Operation types.
 *
 * @see cascading.operation.BaseOperation
 */
public interface Operation
  {
  /** Field ANY denotes that a given Operation will take any number of argument values */
  int ANY = Integer.MAX_VALUE;

  /**
   * The prepare method is called immediately before the current Operation instance is put into play. This method
   * should initialize any resources that can be shutdown or released in the {@link #cleanup(cascading.flow.FlowSession)} method.
   * <p/>
   * This method may be called more than once during the life of this instance. But it will never be called multiple times
   * without a cleanup invocation immediately before subsequent invocations.
   *
   * @param flowSession of type FlowSession
   */
  void prepare( FlowSession flowSession );

  /**
   * The cleanup method is called immediately after the current Operation instance is taken out of play. This method
   * should shutdown any resources created or initialized during the {@link #prepare(cascading.flow.FlowSession)} method.
   * <p/>
   * This method may be called more than once during the life of this instance. But it will never be called multiple times
   * without a prepare invocation before.
   *
   * @param flowSession of type FlowSession
   */
  void cleanup( FlowSession flowSession );

  /**
   * Returns the fields created by this Operation instance. If this instance is a {@link Filter}, it should always
   * return {@link Fields#ALL}.
   *
   * @return
   */
  Fields getFieldDeclaration();

  /**
   * The minimum number of arguments this Operation expects from the calling {@link cascading.pipe.Each} or
   * {@link cascading.pipe.Every} Operator.
   * <p/>
   * Operations should be willing to receive more arguments than expected, but should ignore them if they are unused,
   * instead of failing.
   *
   * @return an int
   */
  int getNumArgs();
  }