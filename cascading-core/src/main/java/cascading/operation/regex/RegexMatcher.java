/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.regex;

import java.beans.ConstructorProperties;
import java.util.regex.Matcher;

import cascading.flow.FlowProcess;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class RegexMatcher is the base class for common regular expression operations.
 * <p/>
 * This operation uses {@link java.util.regex.Matcher} internally, specifically the method {@link java.util.regex.Matcher#find()}.
 *
 * @see java.util.regex.Matcher
 * @see java.util.regex.Pattern
 */
public class RegexMatcher extends RegexOperation<Matcher>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( RegexMatcher.class );

  /** Field removeMatch */
  protected final boolean negateMatch;

  @ConstructorProperties({"patternString"})
  protected RegexMatcher( String patternString )
    {
    super( patternString );
    this.negateMatch = false;
    }

  @ConstructorProperties({"patternString", "negateMatch"})
  protected RegexMatcher( String patternString, boolean negateMatch )
    {
    super( patternString );
    this.negateMatch = negateMatch;
    }

  @ConstructorProperties({"fieldDeclaration", "patternString"})
  protected RegexMatcher( Fields fieldDeclaration, String patternString )
    {
    super( ANY, fieldDeclaration, patternString );
    this.negateMatch = false;

    verify();
    }

  @ConstructorProperties({"fieldDeclaration", "patternString", "negateMatch"})
  protected RegexMatcher( Fields fieldDeclaration, String patternString, boolean negateMatch )
    {
    super( ANY, fieldDeclaration, patternString );
    this.negateMatch = negateMatch;

    verify();
    }

  public boolean isNegateMatch()
    {
    return negateMatch;
    }

  private void verify()
    {
    if( fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "num fields in fieldDeclaration must be one, found: " + fieldDeclaration.printVerbose() );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Matcher> operationCall )
    {
    operationCall.setContext( getPattern().matcher( "" ) );
    }

  /**
   * Method matchWholeTuple ...
   *
   * @param matcher
   * @param input   of type Tuple @return boolean
   */
  protected boolean matchWholeTuple( Matcher matcher, Tuple input )
    {
    matcher.reset( input.toString( "\t", false ) );

    boolean matchFound = matcher.find();

    LOG.debug( "pattern: {}, matches: {}", getPatternString(), matchFound );

    return matchFound == negateMatch;
    }

  /**
   * Method matchEachElement ...
   *
   * @param matcher
   * @param input   of type Tuple @return boolean
   */
  protected boolean matchEachElement( Matcher matcher, Tuple input )
    {
    return matchEachElementPos( matcher, input ) != -1;
    }

  protected int matchEachElementPos( Matcher matcher, Tuple input )
    {
    int pos = 0;
    for( Object value : input )
      {
      if( value == null )
        value = "";

      matcher.reset( value.toString() );

      boolean matchFound = matcher.find();

      if( LOG.isDebugEnabled() )
        LOG.debug( "pattern: " + getPatternString() + ", matches: " + matchFound + ", element: '" + value + "'" );

      if( matchFound == negateMatch )
        return pos;

      pos++;
      }

    return -1;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof RegexMatcher ) )
      return false;
    if( !super.equals( object ) )
      return false;

    RegexMatcher that = (RegexMatcher) object;

    if( negateMatch != that.negateMatch )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( negateMatch ? 1 : 0 );
    return result;
    }
  }
