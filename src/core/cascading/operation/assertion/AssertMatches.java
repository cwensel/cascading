/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.assertion;

import java.beans.ConstructorProperties;
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
  private final static String message = "argument tuple: %s did not match: %s";

  /**
   * Constructor AssertMatches creates a new AssertMatches instance.
   *
   * @param patternString of type String
   */
  @ConstructorProperties({"patternString"})
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
  @ConstructorProperties({"patternString", "negateMatch"})
  public AssertMatches( String patternString, boolean negateMatch )
    {
    super( patternString, negateMatch );
    }

  @Override
  public boolean supportsPlannerLevel( PlannerLevel plannerLevel )
    {
    return plannerLevel instanceof AssertionLevel;
    }

  /** @see cascading.operation.ValueAssertion#doAssert(cascading.flow.FlowProcess, cascading.operation.ValueAssertionCall) */
  public void doAssert( FlowProcess flowProcess, ValueAssertionCall<Matcher> assertionCall )
    {
    Tuple tuple = assertionCall.getArguments().getTuple();

    if( matchWholeTuple( assertionCall.getContext(), tuple ) )
      BaseAssertion.throwFail( message, tuple.print(), patternString );
    }
  }
