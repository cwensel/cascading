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

package cascading.management;

import java.util.List;
import java.util.Map;


/** Class NullDocumentService provides a null implementation. */
public class NullDocumentService implements DocumentService
  {
  @Override
  public void setProperties( Map<Object, Object> properties )
    {
    }

  @Override
  public void startService()
    {
    }

  @Override
  public void stopService()
    {
    }

  @Override
  public void put( String key, Object object )
    {
    }

  @Override
  public void put( String type, String key, Object object )
    {
    }

  @Override
  public Map get( String type, String key )
    {
    return null;
    }

  @Override
  public boolean supportsFind()
    {
    return false;
    }

  @Override
  public List<Map<String, Object>> find( String type, String[] query )
    {
    return null;
    }
  }
