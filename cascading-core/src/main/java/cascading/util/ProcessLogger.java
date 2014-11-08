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

package cascading.util;

/**
 *
 */
public interface ProcessLogger
  {
  ProcessLogger NULL = new ProcessLogger()
  {
  @Override
  public boolean isInfoEnabled()
    {
    return false;
    }

  @Override
  public boolean isDebugEnabled()
    {
    return false;
    }

  @Override
  public void logInfo( String message, Object... arguments )
    {

    }

  @Override
  public void logDebug( String message, Object... arguments )
    {

    }

  @Override
  public void logWarn( String message )
    {

    }

  @Override
  public void logWarn( String message, Object... arguments )
    {

    }

  @Override
  public void logWarn( String message, Throwable throwable )
    {

    }

  @Override
  public void logError( String message, Throwable throwable )
    {

    }
  };

  boolean isInfoEnabled();

  boolean isDebugEnabled();

  void logInfo( String message, Object... arguments );

  void logDebug( String message, Object... arguments );

  void logWarn( String message );

  void logWarn( String message, Object... arguments );

  void logWarn( String message, Throwable throwable );

  void logError( String message, Throwable throwable );
  }