/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.property;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AppPropsTest
  {

  @Test
  public void testApplicationJarPath()
    {
    Map<Object, Object> map = new HashMap<>();
    AppProps.setApplicationJarClass( map, AppPropsTest.class );
    assertEquals( AppPropsTest.class, AppProps.getApplicationJarClass( map ) );

    Properties properties = new Properties();
    AppProps.setApplicationJarClass( properties, AppPropsTest.class );
    assertEquals( AppPropsTest.class, AppProps.getApplicationJarClass( properties ) );
    }
  }
