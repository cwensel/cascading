/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.hadoop;

import cascading.flow.FlowProcess;
import cascading.flow.FlowSession;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

/**
 * Class HadoopFlowSession is an implemenation of {@link FlowSession} for Hadoop. Use this interfact to get direct
 * access to the Hadoop JobConf and Reporter interfaces.
 * <p/>
 * Be warned that coupling to this implemenation, will cause custom {@link cascading.operation.Operation}s to
 * fail if they are executed on a system other than Hadoop.
 *
 * @see cascading.flow.FlowSession
 * @see JobConf
 * @see Reporter
 */
public class HadoopFlowProcess implements FlowProcess
  {
  /** Field jobConf */
  JobConf jobConf;
  /** Field reporter */
  Reporter reporter;

  /**
   * Constructor HFlowSession creates a new HFlowSession instance.
   *
   * @param jobConf of type JobConf
   */
  public HadoopFlowProcess( JobConf jobConf )
    {
    this.jobConf = jobConf;
    }

  /**
   * Method getJobConf returns the jobConf of this HFlowSession object.
   *
   * @return the jobConf (type JobConf) of this HFlowSession object.
   */
  public JobConf getJobConf()
    {
    return jobConf;
    }

  /**
   * Method setReporter sets the reporter of this HFlowSession object.
   *
   * @param reporter the reporter of this HFlowSession object.
   */
  public void setReporter( Reporter reporter )
    {
    this.reporter = reporter;
    }

  /**
   * Method getReporter returns the reporter of this HFlowSession object.
   *
   * @return the reporter (type Reporter) of this HFlowSession object.
   */
  public Reporter getReporter()
    {
    return reporter;
    }

  public Object getProperty( String key )
    {
    return jobConf.get( key );
    }

  public void keepAlive()
    {
    reporter.progress();
    }

  public void increment( Enum counter, int amount )
    {
    reporter.incrCounter( counter, amount );
    }
  }
