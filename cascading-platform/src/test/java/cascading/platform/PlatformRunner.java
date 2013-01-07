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

package cascading.platform;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import cascading.PlatformTestCase;
import cascading.detail.PipeAssemblyTestBase;
import junit.framework.Test;
import org.junit.internal.runners.JUnit38ClassRunner;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class ParentRunner is a JUnit {@link Runner} sub-class for injecting different platform and planners
 * into the *PlatformTest classes.
 * <p/>
 * It works by loading the {@code platform.classname} property from the {@code cascading/platform/platform.properties}
 * resource. Every new platform should provide this resource.
 * <p/>
 * To test against a specific platform, simply make sure the above resource for the platform in question is in the
 * test CLASSPATH. The simplest way is to add it as a dependency.
 */
public class PlatformRunner extends ParentRunner<Runner>
  {
  public static final String PLATFORM_RESOURCE = "cascading/platform/platform.properties";
  public static final String PLATFORM_CLASSNAME = "platform.classname";

  private static final Logger LOG = LoggerFactory.getLogger( PlatformRunner.class );

  private List<Runner> runners;

  public PlatformRunner( Class<PlatformTestCase> testClass ) throws Throwable
    {
    super( testClass );

    makeRunners();
    }

  public static TestPlatform makeInstance( Class<? extends TestPlatform> type )
    {
    try
      {
      return type.newInstance();
      }
    catch( NoClassDefFoundError exception )
      {
      return null;
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

  private List<Runner> makeRunners() throws Throwable
    {
    Class<?> javaClass = getTestClass().getJavaClass();

    runners = new ArrayList<Runner>();

    Set<Class<? extends TestPlatform>> classes = getPlatformClass( javaClass.getClassLoader() );

    for( Class<? extends TestPlatform> platformClass : classes )
      addPlatform( javaClass, platformClass );

    return runners;
    }

  public static Set<Class<? extends TestPlatform>> getPlatformClass( ClassLoader classLoader ) throws IOException, ClassNotFoundException
    {
    Set<Class<? extends TestPlatform>> classes = new HashSet<Class<? extends TestPlatform>>();
    Properties properties = new Properties();

    LOG.info( "classloader: {}", classLoader );

    Enumeration<URL> urls = classLoader.getResources( PLATFORM_RESOURCE );

    while( urls.hasMoreElements() )
      {
      InputStream stream = urls.nextElement().openStream();
      classes.add( (Class<? extends TestPlatform>) getPlatformClass( classLoader, properties, stream ) );
      }

    return classes;
    }

  private static Class<?> getPlatformClass( ClassLoader classLoader, Properties properties, InputStream stream ) throws IOException, ClassNotFoundException
    {
    if( stream == null )
      throw new IllegalStateException( "platform provider resource not found: " + PLATFORM_RESOURCE );

    properties.load( stream );

    String classname = properties.getProperty( PLATFORM_CLASSNAME );

    if( classname == null )
      throw new IllegalStateException( "platform provider value not found: " + PLATFORM_CLASSNAME );

    Class<?> type = classLoader.loadClass( classname );

    if( type == null )
      throw new IllegalStateException( "platform provider class not found: " + classname );

    return type;
    }

  private void addPlatform( final Class<?> javaClass, Class<? extends TestPlatform> type ) throws Throwable
    {
    final TestPlatform testPlatform = makeInstance( type );

    // test platform dependencies not installed, so skip
    if( testPlatform == null )
      return;

    final String platformName = testPlatform.getName();

    LOG.info( "installing platform: {}", platformName );
    LOG.info( "running test: {}", javaClass.getName() );

    if( PipeAssemblyTestBase.class.isAssignableFrom( javaClass ) )
      runners.add( makeSuiteRunner( javaClass, testPlatform ) );
    else
      runners.add( makeClassRunner( javaClass, testPlatform, platformName ) );
    }

  private JUnit38ClassRunner makeSuiteRunner( Class<?> javaClass, final TestPlatform testPlatform ) throws Throwable
    {
    Method method = javaClass.getMethod( "suite", TestPlatform.class );

    return new JUnit38ClassRunner( (Test) method.invoke( null, testPlatform ) );
    }

  private BlockJUnit4ClassRunner makeClassRunner( final Class<?> javaClass, final TestPlatform testPlatform, final String platformName ) throws InitializationError
    {
    return new BlockJUnit4ClassRunner( javaClass )
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
    };
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
