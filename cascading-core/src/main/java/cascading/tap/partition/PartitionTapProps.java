/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.partition;

import java.util.Map;
import java.util.Properties;

import cascading.property.Props;

/**
 * Class PartitionTapProps is a fluent helper class to set properties which control the behaviour of the
 * {@link BasePartitionTap}.
 */
public class PartitionTapProps extends Props
  {
  public static final String FAIL_ON_CLOSE = "cascading.tap.partition.failonclose";

  private boolean failOnClose = false;

  /**
   * Method setFailOnClose(boolean b) controls if the PartitionTap is ignoring all Excpetions, when a TupleEntryCollector
   * is closed or if it should rethrow the Exception.
   *
   * @param properties  a Map<Object, Object>
   * @param failOnClose boolean controlling the close behaviour
   */
  public static void setFailOnClose( Map<Object, Object> properties, boolean failOnClose )
    {
    properties.put( FAIL_ON_CLOSE, Boolean.toString( failOnClose ) );
    }

  /**
   * Creates a new PartitionTapProps instance.
   *
   * @return PartitionTapProps instance
   */
  public static PartitionTapProps partitionTapProps()
    {
    return new PartitionTapProps();
    }

  /**
   * Constructs a new PartitionTapProps instance.
   */
  public PartitionTapProps()
    {
    }

  public boolean isFailOnClose()
    {
    return failOnClose;
    }

  /**
   * Method setFailOnClose controls if the PartitionTap is ignoring all Exceptions, when a TupleEntryCollector
   * is closed or if it should rethrow the Exception.
   *
   * @param failOnClose boolean controlling the close behaviour
   */
  public PartitionTapProps setFailOnClose( boolean failOnClose )
    {
    this.failOnClose = failOnClose;
    return this;
    }

  @Override
  protected void addPropertiesTo( Properties properties )
    {
    setFailOnClose( properties, failOnClose );
    }
  }
