/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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
import cascading.tuple.TupleEntry;

/**
 * Class AssertMatches matches the given regular expression patternString against the whole argument
 * {@link cascading.tuple.Tuple} by joining each individual element of the Tuple with a tab character (\t) unless
 * otherwise specified.
 * See {@link AssertMatchesAll} if you need to match the patternString regex against each individual tuple element.
 * <p/>
 * This operation uses {@link java.util.regex.Matcher} internally, specifically the method {@link java.util.regex.Matcher#find()}.
 * <p/>
 * Note a {@code null} valued argument passed to the parser will be converted to an empty string ({@code ""}) before
 * the regex is applied.
 * <p/>
 * Any Object value will be coerced to a String type via any provided {@link cascading.tuple.type.CoercibleType} on
 * the argument selector or via its {@code toString()} method.
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
   * @param delimiter     of type String
   */
  @ConstructorProperties({"patternString", "delimiter"})
  public AssertMatches( String patternString, String delimiter )
    {
    super( patternString, false, delimiter );
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

  /**
   * Constructor AssertMatches creates a new AssertMatches instance.
   *
   * @param patternString of type String
   * @param negateMatch   of type boolean
   * @param delimiter     of type String
   */
  @ConstructorProperties({"patternString", "negateMatch", "delimiter"})
  public AssertMatches( String patternString, boolean negateMatch, String delimiter )
    {
    super( patternString, negateMatch, delimiter );
    }

  @Override
  public boolean supportsPlannerLevel( PlannerLevel plannerLevel )
    {
    return plannerLevel instanceof AssertionLevel;
    }

  @Override
  public void doAssert( FlowProcess flowProcess, ValueAssertionCall<Matcher> assertionCall )
    {
    TupleEntry tupleEntry = assertionCall.getArguments();

    if( matchWholeTuple( assertionCall.getContext(), tupleEntry ) )
      BaseAssertion.throwFail( message, tupleEntry.getTuple().print(), patternString );
    }
  }
