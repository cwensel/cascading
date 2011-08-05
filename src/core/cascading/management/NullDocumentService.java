/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.management;

import java.util.List;
import java.util.Map;

/**
 *
 */
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
