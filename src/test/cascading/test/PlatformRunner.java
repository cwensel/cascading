/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.test;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cascading.PlatformTestCase;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class PlatformRunner extends ParentRunner<Runner>
  {
  public static final String PLATFORM_INCLUDE = "test.platform.includes";
  private static final Logger LOG = LoggerFactory.getLogger( PlatformRunner.class );

  private Set<String> includes = new HashSet<String>();
  private List<Runner> runners;

  @Retention(RetentionPolicy.RUNTIME)
  public @interface Platform
    {
    Class<? extends TestPlatform>[] value();
    }

  public PlatformRunner( Class<PlatformTestCase> testClass ) throws InitializationError
    {
    super( testClass );

    includes = getIncludes();

    LOG.info( "using includes: {}", includes.toString() );

    makeRunners();
    }

  public static Set<String> getIncludes()
    {
    Set<String> includes = new HashSet<String>();

    if( System.getProperty( PLATFORM_INCLUDE ) != null )
      includes.addAll( Arrays.asList( System.getProperty( PLATFORM_INCLUDE ).split( "," ) ) );

    return includes;
    }

  public static boolean isNotIncluded( Set<String> includes, String platformName )
    {
    return !includes.isEmpty() && !includes.contains( platformName );
    }

  public static TestPlatform makeInstance( Class<? extends TestPlatform> type )
    {
    try
      {
      return type.newInstance();
      }
    catch( InstantiationException exception )
      {
      throw new RuntimeException( exception );
      }
    catch( IllegalAccessException exception )
      {
      throw new RuntimeException( exception );
      }
    }

  @Override
  protected List<Runner> getChildren()
    {
    return runners;
    }

  private List<Runner> makeRunners() throws InitializationError
    {
    Class<?> javaClass = getTestClass().getJavaClass();
    Platform platform = javaClass.getAnnotation( Platform.class );

    runners = new ArrayList<Runner>();

    if( platform == null )
      return runners;

    for( Class<? extends TestPlatform> type : platform.value() )
      {
      final TestPlatform testPlatform = makeInstance( type );
      final String platformName = testPlatform.getName();

      LOG.info( "installing platform: {}", platformName );

      if( isNotIncluded( includes, platformName ) )
        continue;

      runners.add( new BlockJUnit4ClassRunner( javaClass )
      {
      @Override
      protected String getName()
        {
        return String.format( "%s[%s]", super.getName(), platformName );
        }

//        @Override
//        protected String testName( FrameworkMethod method )
//          {
//          return String.format( "%s[%s]", super.testName( method ), platformName );
//          }

      @Override
      protected Object createTest() throws Exception
        {
        PlatformTestCase testCase = (PlatformTestCase) super.createTest();

        testCase.installPlatform( testPlatform );

        return testCase;
        }
      } );
      }

    return runners;
    }

  @Override
  protected Description describeChild( Runner runner )
    {
    return runner.getDescription();
    }

  @Override
  protected void runChild( Runner runner, RunNotifier runNotifier )
    {
    runner.run( runNotifier );
    }
  }
