/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stream;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class TestSinkStage<Incoming> extends Stage<Incoming, Void>
  {
  List<Incoming> results = new ArrayList<Incoming>();

  public TestSinkStage()
    {
    }

  public List<Incoming> getResults()
    {
    return results;
    }

  @Override
  public void bind( StreamGraph streamGraph )
    {
    // do nothing
    }

  @Override
  public void start( Duct previous )
    {
    // do nothing
    }

  @Override
  public void receive( Duct previous, Incoming incoming )
    {
    results.add( incoming );
    }

  @Override
  public void complete( Duct previous )
    {
    // do nothing
    }
  }
