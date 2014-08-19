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

package cascading.tuple;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;

import cascading.flow.FlowProcess;

import cascading.scheme.ConcreteCall;
import cascading.scheme.Scheme;

import cascading.util.CloseableIterator;
import cascading.util.SingleCloseableInputIterator;

/**
 * Class TupleEntrySchemeIterator is a helper class for wrapping a {@link Scheme} instance, calling
 * {@link Scheme#source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)} on every call to {@link #next()}.
 *
 * <p/>Use this class inside a custom {@link cascading.tap.Tap} when overriding the
 * {@link cascading.tap.Tap#openForRead(cascading.flow.FlowProcess)} method.
 */
public class TupleEntrySchemeIterator<Config, Input> extends TupleEntryIterator {
    private final FlowProcess<Config> flowProcess;
    private final Scheme scheme;
    private final CloseableIterator<Input> inputIterator;
    private ConcreteCall sourceCall;

    private String identifier;
    private boolean isComplete = false;
    private boolean hasWaiting = false;
    private TupleException currentException;

    public TupleEntrySchemeIterator(final FlowProcess<Config> flowProcess, final Scheme scheme, final Input input) {
        this(flowProcess, scheme, input, null);
    }

    public TupleEntrySchemeIterator(final FlowProcess<Config> flowProcess, final Scheme scheme, final Input input,
            final String identifier) {
        this(flowProcess, scheme, (CloseableIterator<Input>) new SingleCloseableInputIterator((Closeable) input),
            identifier);
    }

    public TupleEntrySchemeIterator(final FlowProcess<Config> flowProcess, final Scheme scheme,
            final CloseableIterator<Input> inputIterator) {
        this(flowProcess, scheme, inputIterator, null);
    }

    public TupleEntrySchemeIterator(final FlowProcess<Config> flowProcess, final Scheme scheme,
            final CloseableIterator<Input> inputIterator, final String identifier) {
        super(scheme.getSourceFields());
        this.flowProcess = flowProcess;
        this.scheme = scheme;
        this.inputIterator = inputIterator;
        this.identifier = identifier;

        if (this.identifier == null || this.identifier.isEmpty()) {
            this.identifier = "'unknown'";
        }

        if (!inputIterator.hasNext()) {
            isComplete = true;
            return;
        }

        sourceCall = new ConcreteCall();

        sourceCall.setIncomingEntry(getTupleEntry());
        sourceCall.setInput(wrapInput(inputIterator.next()));

        try {
            this.scheme.sourcePrepare(flowProcess, sourceCall);
        } catch (IOException exception) {
            throw new TupleException("unable to prepare source for input identifier: " + this.identifier, exception);
        }
    }

    protected FlowProcess<Config> getFlowProcess() {
        return flowProcess;
    }

    protected Input wrapInput(final Input input) {
        return input;
    }

    @Override
    public boolean hasNext() {
        if (isComplete) {
            return false;
        }

        if (hasWaiting) {
            return true;
        }

        try {
            getNext();
        } catch (Exception exception) {
            if (identifier == null || identifier.isEmpty()) {
                identifier = "'unknown'";
            }

            String message;
            if (exception instanceof EOFException) {
                message = "Unexpected end of file" + identifier + ", will closing file now";
                isComplete = true;
            } else {
                message = "unable to read from input identifier: " + identifier;
            }

            currentException = new TupleException(message, exception);
            return true;
        }

        if (!hasWaiting) {
            isComplete = true;
        }

        return !isComplete;
    }

    private TupleEntry getNext() throws IOException {
        Tuples.asModifiable(sourceCall.getIncomingEntry().getTuple());
        hasWaiting = scheme.source(flowProcess, sourceCall);

        if (!hasWaiting && inputIterator.hasNext()) {
            sourceCall.setInput(wrapInput(inputIterator.next()));

            return getNext();
        }

        return getTupleEntry();
    }

    @Override
    public TupleEntry next() {
        try {
            if (currentException != null) {
                throw currentException;
            }
        } finally {
            currentException = null; // data may be trapped
        }

        if (isComplete) {
            throw new IllegalStateException("no next element");
        }

        try {
            if (hasWaiting) {
                return getTupleEntry();
            }

            return getNext();
        } catch (Exception exception) {
            throw new TupleException("unable to source from input identifier: " + identifier, exception);
        } finally {
            hasWaiting = false;
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("may not remove elements from this iterator");
    }

    @Override
    public void close() throws IOException {
        try {
            if (sourceCall != null) {
                scheme.sourceCleanup(flowProcess, sourceCall);
            }
        } finally {
            inputIterator.close();
        }
    }
}
