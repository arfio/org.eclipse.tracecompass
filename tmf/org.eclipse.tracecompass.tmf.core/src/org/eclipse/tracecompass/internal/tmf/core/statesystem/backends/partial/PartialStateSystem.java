/*******************************************************************************
 * Copyright (c) 2013, 2014 Ericsson
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License 2.0 which
 * accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *   Alexandre Montplaisir - Initial API and implementation
 *******************************************************************************/

package org.eclipse.tracecompass.internal.tmf.core.statesystem.backends.partial;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.tracecompass.internal.statesystem.core.AttributeTree;
import org.eclipse.tracecompass.internal.statesystem.core.StateSystem;
import org.eclipse.tracecompass.statesystem.core.ITmfStateSystem;
import org.eclipse.tracecompass.statesystem.core.ITmfStateSystemBuilder;
import org.eclipse.tracecompass.statesystem.core.backend.IPartialStateHistoryBackend;
import org.eclipse.tracecompass.statesystem.core.exceptions.AttributeNotFoundException;
import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;

/**
 * State system interface-like extension to use with partial state histories.
 *
 * It mainly exposes the {@link #replaceOngoingState} method, which allows
 * seeking the state system to a different point by updating its "ongoing" state
 * values.
 *
 * @author Alexandre Montplaisir
 */
@SuppressWarnings("restriction") /* We're using AttributeTree directly */
public class PartialStateSystem extends StateSystem {

    private static final String ERR_MSG = "Partial state system should not modify the attribute tree!"; //$NON-NLS-1$

    /**
     * Checkpoint attribute name
     */
    public static final String CHECKPOINT_ATTRIBUTE = "_checkpoint"; //$NON-NLS-1$
    private final CountDownLatch fSSAssignedLatch = new CountDownLatch(1);
    private final Lock fQueryLock = new ReentrantLock();
    private int fNbAttributes = 0;

    /**
     * Reference to the real upstream state system. This is used so we can read
     * its attribute tree.
     */
    private @Nullable StateSystem fRealStateSystem = null;

    /**
     * Constructor
     *
     * @param backend
     *            The backend used for storage
     */
    public PartialStateSystem(IPartialStateHistoryBackend backend) {
        /*
         * We use a Null back end here : we only use this state system for its
         * "ongoing" values, so no need to save the changes that are inserted.
         */
        super(backend);
    }

    /**
     * Assign the upstream state system to this one.
     *
     * @param ss
     *            The real state system
     */
    public void assignUpstream(ITmfStateSystemBuilder ss) {
        if (!(ss instanceof StateSystem)) {
            throw new IllegalArgumentException("Partial state system's upstream state system should be of class StateSystem. This one is " + ss.getClass()); //$NON-NLS-1$
        }
        fRealStateSystem = (StateSystem) ss;
        fSSAssignedLatch.countDown();
    }

    @Nullable ITmfStateSystem getUpstreamSS() {
        return fRealStateSystem;
    }

    // ------------------------------------------------------------------------
    // Publicized non-API methods
    // ------------------------------------------------------------------------

    @Override
    public synchronized void replaceOngoingState(List<ITmfStateInterval> ongoingIntervals) {
        super.replaceOngoingState(ongoingIntervals);
    }

    @Override
    public synchronized void dispose() {
        super.dispose();
    }

    // ------------------------------------------------------------------------
    // Methods regarding the query lock
    // ------------------------------------------------------------------------

    /**
     * Take this inner state system's lock before doing a query.
     *
     * When doing queries, you should take the lock, then run
     * {@link #replaceOngoingState}, then send events to its state provider
     * input to cause state changes, and then call {@link #queryOngoingState} to
     * get the states at the new "current time".
     *
     * Only after all that it would be safe to release the lock.
     */
    public void takeQueryLock() {
        try {
            fQueryLock.lockInterruptibly();
        } catch (InterruptedException e) {
            // Do nothing
        }
    }

    /**
     * Release the query lock, when you are done with your query.
     */
    public void releaseQueryLock() {
        fQueryLock.unlock();
    }

    @Override
    public AttributeTree getAttributeTree() {
        waitUntilReady();
        return Objects.requireNonNull(Objects.requireNonNull(fRealStateSystem).getAttributeTree());
    }

    public void setNbAttributes(int nbAttributes) {
        fNbAttributes = nbAttributes;
    }

    @Override
    public int getNbAttributes() {
        return fNbAttributes;
    }

    /*
     * Override these methods to make sure we don't try to overwrite the
     * "real" upstream attribute tree.
     */
    @Override
    public synchronized void addEmptyAttribute() {
        throw new RuntimeException(ERR_MSG);
    }

    @Override
    public int getQuarkAbsoluteAndAdd(String @Nullable... attribute) {
        waitUntilReady();
        try {
            if (attribute != null && attribute[0].equals(CHECKPOINT_ATTRIBUTE)) {
                return Objects.requireNonNull(fRealStateSystem).getQuarkAbsoluteAndAdd(attribute);
            }
            return Objects.requireNonNull(fRealStateSystem).getQuarkAbsolute(attribute);
        } catch (AttributeNotFoundException e) {
            throw new RuntimeException(ERR_MSG);
        }
    }

    @Override
    public int getQuarkRelativeAndAdd(int startingNodeQuark, String @Nullable... subPath) {
        waitUntilReady();
        try {
            return Objects.requireNonNull(fRealStateSystem).getQuarkRelative(startingNodeQuark, subPath);
        } catch (AttributeNotFoundException e) {
            throw new RuntimeException(ERR_MSG);
        }
    }

    private void waitUntilReady() {
        try {
            fSSAssignedLatch.await();
        } catch (InterruptedException e) {
            // Do nothing
        }
    }
}
