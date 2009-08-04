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

package cascading.operation.assertion;

import java.util.regex.Matcher;

import cascading.flow.FlowProcess;
import cascading.operation.AssertionLevel;
import cascading.operation.PlannerLevel;
import cascading.operation.ValueAssertion;
import cascading.operation.ValueAssertionCall;
import cascading.operation.regex.RegexMatcher;
import cascading.tuple.Tuple;

/**
 * Class AssertMatches matches the given regular expression patternString against the whole argument
 * {@link cascading.tuple.Tuple} by joining each individual element of the Tuple with a tab character (\t).
 * See {@link AssertMatchesAll} if you need to match the patternString regex against each individual tuple element.
 * <p/>
 * This operation uses {@link java.util.regex.Matcher} internally, specifically the method {@link java.util.regex.Matcher#find()}.
 *
 * @see java.util.regex.Matcher
 * @see java.util.regex.Pattern
 */
public class AssertMatches extends RegexMatcher implements ValueAssertion<Matcher>
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

  @Override
  public boolean supportsPlannerLevel( PlannerLevel plannerLevel )
    {
    return plannerLevel instanceof AssertionLevel;
    }

  /** @see cascading.operation.ValueAssertion#doAssert(cascading.flow.FlowProcess,cascading.operation.ValueAssertionCall) */
  public void doAssert( FlowProcess flowProcess, ValueAssertionCall<Matcher> assertionCall )
    {
    Tuple tuple = assertionCall.getArguments().getTuple();

    if( matchWholeTuple( assertionCall.getContext(), tuple ) )
      BaseAssertion.throwFail( message, tuple.print(), patternString );
    }
  }
