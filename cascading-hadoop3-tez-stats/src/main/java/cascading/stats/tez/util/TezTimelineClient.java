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

package cascading.stats.tez.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

import cascading.CascadingException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGClientTimelineImpl;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.tez.common.ATSConstants.*;
import static org.apache.tez.dag.history.logging.EntityTypes.TEZ_TASK_ID;

/**
 * TezTimelineClient is currently just a proxy around DAGClient that pretends to
 * implement TimelineClient (throws errors on all methods).
 *
 * When TEZ-3369 is implemented, TezTimelineClient can be replaced by a pure DAGClient usage. Until then,
 * FlowStats is non-functional.
 */
public class TezTimelineClient extends DAGClient implements TimelineClient {

    private final String dagId;
    private final FrameworkClient frameworkClient;
    private final DAGClient dagClient;

    public TezTimelineClient(ApplicationId appId, String dagId, TezConfiguration conf, FrameworkClient frameworkClient, DAGClient dagClient) throws TezException {
        this.dagId = dagId;
        this.frameworkClient = frameworkClient;
        this.dagClient = dagClient;
    }

    public DAGClient getDAGClient() {
        return dagClient;
    }

    public FrameworkClient getFrameworkClient() {
        return frameworkClient;
    }


    @Override
    public DAGStatus getDAGStatus(@Nullable Set<StatusGetOpts> statusOptions) throws IOException, TezException {
        return dagClient.getDAGStatus(statusOptions);
    }

    @Override
    public DAGStatus getDAGStatus(@Nullable Set<StatusGetOpts> statusOptions, long timeout) throws IOException, TezException {
        return dagClient.getDAGStatus(statusOptions, timeout);
    }

    @Override
    public VertexStatus getVertexStatus(String vertexName, Set<StatusGetOpts> statusOptions) throws IOException, TezException {
        return dagClient.getVertexStatus(vertexName, statusOptions);
    }

    @Override
    public void tryKillDAG() throws IOException, TezException {
        dagClient.tryKillDAG();
    }

    @Override
    public DAGStatus waitForCompletion() throws IOException, TezException, InterruptedException {
        return dagClient.waitForCompletion();
    }

    @Override
    public void close() throws IOException {
        dagClient.close();
    }

    @Override
    public DAGStatus waitForCompletionWithStatusUpdates(@Nullable Set<StatusGetOpts> statusOpts) throws IOException, TezException, InterruptedException {
        return dagClient.waitForCompletionWithStatusUpdates(statusOpts);
    }

    @Override
    public String getSessionIdentifierString() {
        return dagClient.getSessionIdentifierString();
    }

    @Override
    public String getDagIdentifierString() {
        return dagClient.getDagIdentifierString();
    }

    @Override
    public String getExecutionContext() {
        return dagClient.getExecutionContext();
    }

    @Override
    public String getVertexID(String vertexName) throws IOException, TezException {
        throw new TezException("reporting API is temporarily disabled on TEZ-3369 implementation");
    }

    @Override
    public Iterator<TaskStatus> getVertexChildren(String vertexID, int limit, String startTaskID) throws IOException, TezException {
        throw new TezException("reporting API is temporarily disabled on TEZ-3369 implementation");
    }

    @Override
    public TaskStatus getVertexChild(String taskID) throws TezException {
        throw new TezException("reporting API is temporarily disabled on TEZ-3369 implementation");
    }

    @Override
    protected ApplicationReport getApplicationReportInternal() {
        return null; // not implemented
    }

}
