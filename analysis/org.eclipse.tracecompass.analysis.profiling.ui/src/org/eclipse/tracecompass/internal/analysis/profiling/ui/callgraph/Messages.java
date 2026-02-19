/*******************************************************************************
 * Copyright (c) 2016 Ericsson
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *******************************************************************************/

package org.eclipse.tracecompass.internal.analysis.profiling.ui.callgraph;

import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.osgi.util.NLS;

/**
 * Message bundle for the call stack views
 *
 * @author Sonia Farrah
 */
public class Messages extends NLS {
    private static final String BUNDLE_NAME = Messages.class.getPackage().getName() + ".messages"; //$NON-NLS-1$
    /**
     * Name of the job executing the callGraphAnalysis
     */
    public static @Nullable String CallGraphAnalysis;

    /**
     * Build Job
     */
    public static String CallGraphView_BuildJob;
    /**
     * Group for layout buttons
     */
    public static String CallGraphView_Group;

    /**
     * Horizontal tree layout
     */
    public static String CallGraphView_HorizontalTreeLayout;

    /**
     * Layout job
     */
    public static String CallGraphView_LayoutJob;

    /**
     * Radial layout
     */
    public static String CallGraphView_RadialLayout;

    /**
     * Spring layout
     */
    public static String CallGraphView_SpringLayout;

    /**
     * Symbol job name
     */
    public static String CallGraphView_SymbolJobName;

    /**
     * Tree layout
     */
    public static String CallGraphView_TreeLayout;

    /**
     * Wait for parent message
     */
    public static String CallGraphView_WaitForParentAnalysis;

    static {
        // initialize resource bundle
        NLS.initializeMessages(BUNDLE_NAME, Messages.class);
    }

    private Messages() {
    }
}
