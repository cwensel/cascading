/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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
 * Class AssertMatchesAll matches the given regular expression patternString against each argument
 * {@link cascading.tuple.Tuple} element individually. See {@link AssertMatches} if you need to match the patternString regex against
 * the tuple as a whole.
 * <p/>
 * This operation uses {@link java.util.regex.Matcher} internally, specifically the method {@link java.util.regex.Matcher#find()}.
 *
 * @see java.util.regex.Matcher
 * @see java.util.regex.Pattern
 */
public class AssertMatchesAll extends RegexMatcher implements ValueAssertion<Matcher>
  {
  /** Field message */
  private final static String message = "argument '%s' value was: %s, did not match: %s, in tuple: %s";

  /**
   * Constructor AssertMatchesAll creates a new AssertMatchesAll instance.
   *
   * @param patternString of type String
   */
  @ConstructorProperties({"patternString"})
  public AssertMatchesAll( String patternString )
    {
    super( patternString, false );
    }

  /**
   * Constructor AssertMatchesAll creates a new AssertMatchesAll instance.
   *
   * @param patternString of type String
   * @param negateMatch   of type boolean
   */
  @ConstructorProperties({"patternString", "negateMatch"})
  public AssertMatchesAll( String patternString, boolean negateMatch )
    {
    super( patternString, negateMatch );
    }

  @Override
  public boolean supportsPlannerLevel( PlannerLevel plannerLevel )
    {
    return plannerLevel instanceof AssertionLevel;
    }

  @Override
  public void doAssert( FlowProcess flowProcess, ValueAssertionCall<Matcher> assertionCall )
    {
    TupleEntry input = assertionCall.getArguments();

    int pos = matchEachElementPos( assertionCall.getContext(), input.getTuple() );

    if( pos != -1 )
      BaseAssertion.throwFail( message, input.getFields().get( pos ), input.getObject( pos ), patternString, input.getTuple().print() );
    }
  }