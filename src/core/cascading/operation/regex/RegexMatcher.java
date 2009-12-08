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

package cascading.operation.regex;

import java.beans.ConstructorProperties;
import java.util.regex.Matcher;

import cascading.flow.FlowProcess;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.log4j.Logger;

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
  private static final Logger LOG = Logger.getLogger( RegexMatcher.class );
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
    matcher.reset( input.toString( "\t" ) );

    boolean matchFound = matcher.find();

    if( LOG.isDebugEnabled() )
      LOG.debug( "pattern: " + getPattern() + ", matches: " + matchFound );

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
        LOG.debug( "pattern: " + getPattern() + ", matches: " + matchFound + ", element: '" + value + "'" );

      if( matchFound == negateMatch )
        return pos;

      pos++;
      }

    return -1;
    }
  }
