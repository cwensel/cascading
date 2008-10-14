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
import cascading.tuple.TupleEntry;

/**
 * Class GroupAssertion is a kind of {@link Assertion} used with the {@link cascading.pipe.Every} pipe Operator.
 * <p/>
 * Implementors must also extend {@link BaseOperation}.
 */
public interface GroupAssertion<C> extends Assertion
  {
  /**
   * Method start initializes the aggregation procedure and is called for every unique grouping.
   * <p/>
   * The context is used to hold intermediate values is is user defined. The context should be initialized here if necessary.
   * <p/>
   * This method will be called before {@link #aggregate(cascading.flow.FlowSession, GroupAssertionCall)}
   * and {@link #aggregate(cascading.flow.FlowSession, GroupAssertionCall)}
   * <p/>
   * TupleEntry groupEntry, or groupEntry.getTuple() should not be stored directly in the context. A copy of the tuple
   * should be made via the {@code new Tuple( entry.getTuple() )} copy constructor if the whole Tuple is kept.
   * <p/>
   * The first time this method is called for a given 'session', context will be null. This method should return a
   * new instance of the user defined context object. If context is not null, it is up to the developer to create a
   * new instance, or 'recycle' the given instance. If recycled, it must be re-initialized to remove any
   * previous state/values.
   * <p/>
   * For example, if a Map is used to hold the intermediate data for each subsequent
   * {@link #aggregate(cascading.flow.FlowSession, GroupAssertionCall)}
   * call, new HashMap() should be returned when context is null. On the next grouping, start() will be called
   * again, but this time with the old Map instance. In this case, map.clear() should be called before returning the
   * instance.
   *
   * @param flowSession   of type FlowSession is the current session
   * @param assertionCall
   * @return is user defined
   */
  void start( FlowSession flowSession, GroupAssertionCall<C> assertionCall );

  /**
   * Method aggregate is called for each {@link TupleEntry} value in the current grouping.
   *
   * @param flowSession   of type FlowSession
   * @param assertionCall
   */
  void aggregate( FlowSession flowSession, GroupAssertionCall<C> assertionCall );

  /**
   * Method doAssert performs the assertion.
   *
   * @param flowSession   of type FlowSession
   * @param assertionCall
   */
  void doAssert( FlowSession flowSession, GroupAssertionCall<C> assertionCall );

  }