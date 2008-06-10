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

package cascading.operation.assertion;

import cascading.operation.Assertion;
import cascading.operation.regex.RegexMatcher;
import cascading.tuple.TupleEntry;

/**
 * Class AssertMatches matches the given regular expression patternString against the whole argument
 * {@link cascading.tuple.Tuple} by joining each individual element of the Tuple with a tab character (\t).
 * See {@link AssertMatchesAll} if you need to match the patternString regex against each individual tuple element.
 */
public class AssertMatches extends RegexMatcher implements Assertion
  {
  /** Field message */
  private final String message = "argument tuple: %s did not match: %s";

  /**
   * Constructor AssertMatches creates a new AssertMatches instance.
   *
   * @param patternString of type String
   */
  public AssertMatches( String patternString )
    {
    super( patternString, false );
    }

  /**
   * Constructor AssertMatches creates a new AssertMatches instance.
   *
   * @param patternString of type String
   * @param negateMatch   of type boolean
   */
  public AssertMatches( String patternString, boolean negateMatch )
    {
    super( patternString, negateMatch );
    }

  /** @see cascading.operation.Assertion#doAssert(TupleEntry) */
  public void doAssert( TupleEntry input )
    {
    if( matchWholeTuple( input.getTuple() ) )
      BaseAssertion.throwFail( message, input.getTuple().print(), patternString );
    }
  }
