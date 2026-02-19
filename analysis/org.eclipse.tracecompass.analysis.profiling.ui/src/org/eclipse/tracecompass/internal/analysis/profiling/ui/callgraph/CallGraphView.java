/*******************************************************************************
 * Copyright (c) 2018 Ericsson
 *
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/
package org.eclipse.tracecompass.internal.analysis.profiling.ui.callgraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.NullProgressMonitor;
import org.eclipse.core.runtime.Status;
import org.eclipse.core.runtime.SubMonitor;
import org.eclipse.core.runtime.jobs.Job;
import org.eclipse.draw2d.Label;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.Nullable;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.ActionContributionItem;
import org.eclipse.jface.action.IAction;
import org.eclipse.jface.action.IMenuCreator;
import org.eclipse.jface.resource.ColorRegistry;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.tracecompass.analysis.profiling.core.base.ICallStackElement;
import org.eclipse.tracecompass.analysis.profiling.core.base.ICallStackSymbol;
import org.eclipse.tracecompass.analysis.profiling.core.callgraph.AggregatedCallSite;
import org.eclipse.tracecompass.analysis.profiling.core.callgraph.CallGraph;
import org.eclipse.tracecompass.analysis.profiling.core.callgraph.ICallGraphProvider2;
import org.eclipse.tracecompass.analysis.profiling.core.tree.ITree;
import org.eclipse.tracecompass.common.core.NonNullUtils;
import org.eclipse.tracecompass.common.core.log.TraceCompassLog;
import org.eclipse.tracecompass.common.core.log.TraceCompassLogUtils;
import org.eclipse.tracecompass.common.core.log.TraceCompassLogUtils.FlowScopeLog;
import org.eclipse.tracecompass.common.core.log.TraceCompassLogUtils.FlowScopeLogBuilder;
import org.eclipse.tracecompass.internal.analysis.profiling.core.callgraph2.AggregatedCalledFunction;
import org.eclipse.tracecompass.tmf.core.analysis.IAnalysisModule;
import org.eclipse.tracecompass.tmf.core.signal.TmfSignalHandler;
import org.eclipse.tracecompass.tmf.core.signal.TmfTraceSelectedSignal;
import org.eclipse.tracecompass.tmf.core.symbols.SymbolProviderManager;
import org.eclipse.tracecompass.tmf.core.trace.ITmfTrace;
import org.eclipse.tracecompass.tmf.core.trace.TmfTraceManager;
import org.eclipse.tracecompass.tmf.core.trace.TmfTraceUtils;
import org.eclipse.tracecompass.tmf.ui.editors.ITmfTraceEditor;
import org.eclipse.tracecompass.tmf.ui.symbols.TmfSymbolProviderUpdatedSignal;
import org.eclipse.tracecompass.tmf.ui.views.TmfView;
import org.eclipse.ui.IEditorPart;
import org.eclipse.zest.core.viewers.AbstractZoomableViewer;
import org.eclipse.zest.core.viewers.GraphViewer;
import org.eclipse.zest.core.viewers.IZoomableWorkbenchPart;
import org.eclipse.zest.core.viewers.ZoomContributionViewItem;
import org.eclipse.zest.core.widgets.Graph;
import org.eclipse.zest.core.widgets.GraphConnection;
import org.eclipse.zest.core.widgets.GraphItem;
import org.eclipse.zest.core.widgets.GraphNode;
import org.eclipse.zest.core.widgets.ZestStyles;
import org.eclipse.zest.layouts.LayoutAlgorithm;
import org.eclipse.zest.layouts.LayoutStyles;
import org.eclipse.zest.layouts.algorithms.HorizontalTreeLayoutAlgorithm;
import org.eclipse.zest.layouts.algorithms.RadialLayoutAlgorithm;
import org.eclipse.zest.layouts.algorithms.SpringLayoutAlgorithm;
import org.eclipse.zest.layouts.algorithms.TreeLayoutAlgorithm;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;

/**
 * This view displays a call graph
 *
 * @author Sonia Farrah
 */
public class CallGraphView extends TmfView implements IZoomableWorkbenchPart {

    /**
     *
     */
    private static final @NonNull Level LEVEL = Level.FINE;
    /**
     * LOGGER
     */
    private static final @NonNull Logger LOGGER = TraceCompassLog.getLogger(CallGraphView.class);
    private static final String GROUP_NAME = Messages.CallGraphView_Group;
    private static final @NonNull String CATEGORY = CallGraphView.class.getSimpleName();
    private static final RGB[] COLORS = new RGB[360];
    private static final ColorRegistry COLOR_REGISTRY = new ColorRegistry();
    static {
        for (int i = 0; i < COLORS.length; i++) {
            COLORS[i] = new RGB(i, 0.6f, 0.6f);
            COLOR_REGISTRY.put(COLORS[i].toString(), COLORS[i]);
        }
    }

    /**
     * Call Graph Node, wrapper for a callsite
     */
    private static final class CallGraphNode {
        private final ICallStackSymbol fSymbol;
        private long fTime = 0;
        private final Multiset<ICallStackSymbol> fCallers = HashMultiset.create();
        private final Multiset<ICallStackSymbol> fCallees = HashMultiset.create();

        /**
         * Constructor
         *
         * @param parent
         *            parent node, can be null
         * @param callSite
         *            node to wrap
         *
         */
        public CallGraphNode(AggregatedCallSite parent, AggregatedCallSite callSite) {
            fSymbol = callSite.getObject();
            update(parent, callSite);
        }

        public void update(AggregatedCallSite parent, AggregatedCallSite callSite) {
            if (callSite instanceof AggregatedCalledFunction) {
                AggregatedCalledFunction aggregatedCalledFunction = (AggregatedCalledFunction) callSite;
                fTime = aggregatedCalledFunction.getDuration();
            } else {
                fTime++;
            }
            if (parent != null) {
                fCallers.add(parent.getObject());
            }
            fCallees.addAll(callSite.getCallees().stream().map(AggregatedCallSite::getObject).collect(Collectors.toList()));
        }

        /**
         * @return the time
         */
        public long getTime() {
            return fTime;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("CallGraphNode ").append(fSymbol).append("\n duration=").append(fTime).append("\n Callers=").append(Joiner.on('\n').join(fCallers.entrySet())).append("\n Callees=").append(Joiner.on('\n').join(fCallees.entrySet())); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
            return builder.toString();
        }

    }

    private final class CallGraphPopulator extends Job {
        private final @NonNull CallGraph fCallGraph;
        private final @Nullable ITmfTrace fCallGraphTrace;
        private final @NonNull FlowScopeLog fParentScope;

        public CallGraphPopulator(String name, @NonNull CallGraph callGraph, @NonNull FlowScopeLog parentScope) {
            super(name);
            fCallGraph = callGraph;
            fParentScope = parentScope;
            fCallGraphTrace = getTrace();
        }

        @Override
        protected IStatus run(@Nullable IProgressMonitor monitor) {
            try (FlowScopeLog jobLog = new FlowScopeLogBuilder(LOGGER, LEVEL, "GraphBuilder").setParentScope(fParentScope).build()) { //$NON-NLS-1$
                if (monitor == null || monitor.isCanceled() || fCallGraphTrace == null) {
                    return Status.CANCEL_STATUS;
                }
                CallGraph callGraph = fCallGraph;

                Map<ICallStackElement, Collection<AggregatedCallSite>> ccts = new LinkedHashMap<>();
                populate(callGraph, callGraph.getElements(), ccts);

                Map<ICallStackElement, Map<ICallStackSymbol, CallGraphNode>> callgraph = convert(ccts);
                Map<ICallStackElement, Long> totals = new HashMap<>();
                for (Entry<ICallStackElement, Map<ICallStackSymbol, CallGraphNode>> entry : callgraph.entrySet()) {
                    Collection<CallGraphNode> nodes = entry.getValue().values();
                    long totaltime = nodes.stream().collect(Collectors.summingLong(CallGraphNode::getTime));
                    totals.put(entry.getKey(), totaltime);
                }

                Display.getDefault().asyncExec(() -> {
                    try (FlowScopeLog displayLog = new FlowScopeLogBuilder(LOGGER, LEVEL, "GraphBuilder#Display").setParentScope(jobLog).build()) { //$NON-NLS-1$
                        clear(monitor);
                        if (fGraph.isDisposed()) {
                            throw new IllegalStateException();
                        }
                        for (Entry<ICallStackElement, Map<ICallStackSymbol, CallGraphNode>> cct : callgraph.entrySet()) {
                            displayLog.step(cct.getKey().getName());
                            Collection<CallGraphNode> nodes = cct.getValue().values();
                            /*
                             * Make nodes
                             */
                            try (FlowScopeLog nodesLog = new FlowScopeLogBuilder(LOGGER, LEVEL, "createNodes").setParentScope(jobLog).build()) { //$NON-NLS-1$

                                for (CallGraphNode function : nodes) {
                                    if (function == null) {
                                        continue;
                                    }
                                    ICallStackSymbol symbol = Objects.requireNonNull(function.fSymbol);
                                    getNodes().putIfAbsent(symbol, new GraphNode(fGraph, SWT.NONE, function));
                                }
                            }
                            /*
                             * Connect them
                             */
                            try (FlowScopeLog nodesLog = new FlowScopeLogBuilder(LOGGER, LEVEL, "createLinks").setParentScope(jobLog).build()) { //$NON-NLS-1$

                                for (CallGraphNode function : nodes) {
                                    GraphNode functionNode = getNodes().get(function.fSymbol);
                                    Multiset<ICallStackSymbol> callees = function.fCallees;
                                    for (Multiset.Entry<ICallStackSymbol> callee : callees.entrySet()) {
                                        GraphNode child = getNodes().get(callee.getElement());
                                        if (child == null) {
                                            continue;
                                        }

                                        GraphConnection cnnx = new GraphConnection(fGraph, ZestStyles.CONNECTIONS_DIRECTED, functionNode, child);
                                        cnnx.setText(String.valueOf(callee.getCount()));
                                        int lineWidth = (int) (Math.max(1.0, ((CallGraphNode) (child.getData())).getTime() * 5.0 / (getTrace().getEndTime().toNanos() - getTrace().getStartTime().toNanos())));
                                        cnnx.setLineWidth(lineWidth);
                                        cnnx.setWeight(lineWidth);
                                        getConnections().add(cnnx);
                                    }
                                }
                            }
                        }
                        Job symbolJob = new Job(Messages.CallGraphView_SymbolJobName) {
                            @Override
                            public IStatus run(IProgressMonitor ipm) {
                                TraceCompassLogUtils.traceCounter(LOGGER, LEVEL, "CallGraph", "nodes", fGraph.getNodes().size(), "connections", fGraph.getConnections().size()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
                                innerSymbolUpdated(displayLog);
                                return Status.OK_STATUS;
                            }
                        };
                        symbolJob.setSystem(true);
                        symbolJob.schedule();
                    }
                });

                return Status.OK_STATUS;
            }
        }

        /**
         * @param ccts
         * @return
         */
        private Map<ICallStackElement, Map<ICallStackSymbol, CallGraphNode>> convert(Map<ICallStackElement, Collection<AggregatedCallSite>> ccts) {
            Map<ICallStackElement, Map<ICallStackSymbol, CallGraphNode>> ret = new LinkedHashMap<>();
            for (Entry<ICallStackElement, Collection<AggregatedCallSite>> cct : ccts.entrySet()) {
                ICallStackElement key = cct.getKey();
                Map<ICallStackSymbol, CallGraphNode> nodes = new HashMap<>();
                convert(nodes, null, cct.getValue());
                ret.put(key, nodes);
            }
            return ret;
        }

        /**
         * @param callGraphNodes
         * @param value
         */
        private void convert(Map<ICallStackSymbol, CallGraphNode> callGraphNodes, AggregatedCallSite parent, Collection<AggregatedCallSite> value) {
            for (AggregatedCallSite acs : value) {
                CallGraphNode node = callGraphNodes.get(acs.getObject());
                if (node != null) {
                    node.update(parent, acs);
                } else {
                    callGraphNodes.put(acs.getObject(), new CallGraphNode(parent, acs));
                }
                convert(callGraphNodes, acs, acs.getCallees());
            }

        }

        private void populate(CallGraph callGraph, Collection<? extends ITree> elements, Map<ICallStackElement, Collection<AggregatedCallSite>> ccts) {
            for (ITree tree : elements) {
                if (!(tree instanceof ICallStackElement)) {
                    continue;
                }
                ICallStackElement element = (ICallStackElement) tree;
                Collection<@NonNull AggregatedCallSite> callingContextTree = callGraph.getCallingContextTree(element);
                ccts.put(element, flatten(callingContextTree));
                populate(callGraph, element.getChildren(), ccts);
            }
        }

        /**
         * @param callingContextTree
         * @return
         */
        private Collection<AggregatedCallSite> flatten(Collection<@NonNull AggregatedCallSite> callingContextTree) {
            Set<AggregatedCallSite> ret = new HashSet<>();
            for (AggregatedCallSite aggr : callingContextTree) {
                ret.addAll(flatten(aggr.getCallees()));
            }
            ret.addAll(callingContextTree);
            return ret;
        }
    }

    /**
     * ID
     */
    public static final @NonNull String ID = CallGraphView.class.getPackage().getName() + ".callgraph"; //$NON-NLS-1$
    private ITmfTrace fTrace;
    private GraphViewer fGraphViewer;
    private Graph fGraph;
    private String fAnalysisId;
    private final Map<@NonNull ICallStackSymbol, GraphNode> fNodes = new LinkedHashMap<>();
    private final Collection<GraphConnection> fCnnx = new ArrayList<>();
    private IAction fLayoutAction;
    private Color fTextColor;

    /**
     * Constructor
     */
    public CallGraphView() {
        super(ID);
    }

    @Override
    public void dispose() {
        clear(null);
        super.dispose();
    }

    private void clear(IProgressMonitor monitor) {
        int work = fGraph.getConnections().size() + fGraph.getNodes().size();
        IProgressMonitor subMon = (monitor == null) ? new NullProgressMonitor() : SubMonitor.convert(monitor, work);
        List<GraphItem> victims = new ArrayList<>();
        for (Object item : fGraph.getConnections()) {
            if (item instanceof GraphItem) {
                victims.add((GraphItem) item);
            }
            subMon.worked(1);
            if (subMon.isCanceled()) {
                break;
            }
        }
        for (Object item : fGraph.getNodes()) {
            if (item instanceof GraphItem) {
                victims.add((GraphItem) item);
            }
            subMon.worked(1);
            if (subMon.isCanceled()) {
                break;
            }
        }
        victims.forEach(GraphItem::dispose);
        getConnections().clear();
        getNodes().clear();
    }

    @Override
    public void createPartControl(Composite parent) {
        try (FlowScopeLog flowScope = new FlowScopeLogBuilder(LOGGER, LEVEL, "CreatePartControl").setCategory(CATEGORY).build()) { //$NON-NLS-1$
            fAnalysisId = NonNullUtils.nullToEmptyString(getViewSite().getSecondaryId());
            fGraphViewer = new GraphViewer(parent, SWT.V_SCROLL | SWT.H_SCROLL);

            fGraph = fGraphViewer.getGraphControl();
            fTextColor = parent.getDisplay().getSystemColor(SWT.COLOR_WHITE);
            IEditorPart editor = getSite().getPage().getActiveEditor();
            if (editor instanceof ITmfTraceEditor) {
                ITmfTrace trace = ((ITmfTraceEditor) editor).getTrace();
                if (trace != null) {
                    traceSelected(new TmfTraceSelectedSignal(this, trace));
                }
            }
            LayoutAlgorithm treeAlgorithm = new TreeLayoutAlgorithm(LayoutStyles.NO_LAYOUT_NODE_RESIZING);
            LayoutAlgorithm hTreeAlgorithm = new HorizontalTreeLayoutAlgorithm(LayoutStyles.NO_LAYOUT_NODE_RESIZING);
            LayoutAlgorithm radialAlgorithm = new RadialLayoutAlgorithm(LayoutStyles.NO_LAYOUT_NODE_RESIZING);
            LayoutAlgorithm springAlgorithm = new SpringLayoutAlgorithm(LayoutStyles.NO_LAYOUT_NODE_RESIZING);
            fGraph.setLayoutAlgorithm(treeAlgorithm, true);
            setTrace(TmfTraceManager.getInstance().getActiveTrace());
            List<IAction> actions = ImmutableList.of(
                    createLayoutAction(Messages.CallGraphView_SpringLayout, springAlgorithm),
                    createLayoutAction(Messages.CallGraphView_RadialLayout, radialAlgorithm),
                    createLayoutAction(Messages.CallGraphView_TreeLayout, treeAlgorithm),
                    createLayoutAction(Messages.CallGraphView_HorizontalTreeLayout, hTreeAlgorithm));

            actions.get(2).setChecked(true);
            fLayoutAction = new Action(GROUP_NAME, IAction.AS_DROP_DOWN_MENU) {
                @Override
                public void run() {
                    innerSymbolUpdated(flowScope);
                }
            };

            fLayoutAction.setMenuCreator(new IMenuCreator() {

                Menu menu = null;

                @Override
                public void dispose() {
                    if (menu != null) {
                        menu.dispose();
                        menu = null;
                    }
                }

                @Override
                public Menu getMenu(Control unused) {
                    if (menu != null) {
                        menu.dispose();
                    }
                    menu = new Menu(parent);
                    for (IAction action : actions) {
                        new ActionContributionItem(action).fill(menu, -1);
                    }
                    return menu;
                }

                @Override
                public Menu getMenu(Menu unused) {
                    if (menu != null) {
                        menu.dispose();
                    }
                    menu = new Menu(parent);
                    for (IAction action : actions) {
                        new ActionContributionItem(action).fill(menu, -1);
                    }
                    return menu;
                }

            }

            );
            getViewSite().getActionBars().getToolBarManager().add(fLayoutAction);
            getViewSite().getActionBars().getToolBarManager().add(new ZoomContributionViewItem(this));
        }
    }

    private @NonNull Action createLayoutAction(@Nullable String layoutName, @NonNull LayoutAlgorithm layoutAlgorithm) {
        return new Action(layoutName, IAction.AS_RADIO_BUTTON) {
            @Override
            public void runWithEvent(Event event) {
                fGraph.setLayoutAlgorithm(layoutAlgorithm, true);
                fGraph.applyLayout();
                if (fLayoutAction != null) {
                    fLayoutAction.setText(getText());
                }
            }
        };
    }

    /**
     * Handler for the trace selected signal
     *
     * @param signal
     *            The incoming signal
     */
    @TmfSignalHandler
    public void traceSelected(final TmfTraceSelectedSignal signal) {
        if (getTrace() != signal.getTrace()) {
            setTrace(signal.getTrace());
            try (FlowScopeLog fs = new FlowScopeLogBuilder(LOGGER, LEVEL, "traceSelected").setCategory(CATEGORY).build()) { //$NON-NLS-1$
                buildCallGraph(fs);
            }
        }
    }

    /**
     * Symbol mapper updated
     *
     * @param signal
     *            the symbol provider is updated
     */
    @TmfSignalHandler
    public void symbolsUpdated(final TmfSymbolProviderUpdatedSignal signal) {
        try (FlowScopeLog fs = new FlowScopeLogBuilder(LOGGER, LEVEL, "symbolsUpdated").setCategory(CATEGORY).build()) { //$NON-NLS-1$
            innerSymbolUpdated(fs);
        }
    }

    private void innerSymbolUpdated(@NonNull FlowScopeLog flowScope) {
        Set<ICallStackSymbol> symbols = new HashSet<>();
        Display.getDefault().syncExec(() -> {
            try (FlowScopeLog dfs = new FlowScopeLogBuilder(LOGGER, LEVEL, "innerSymbolUpdated#getSymbols").setParentScope(flowScope).build()) { //$NON-NLS-1$
                for (Object value : fGraph.getNodes()) {
                    if (value instanceof GraphNode) {
                        GraphNode gn = (GraphNode) value;
                        CallGraphNode cgn = (CallGraphNode) gn.getData();
                        symbols.add(cgn.fSymbol);
                    }
                }
            }
        });

        final Map<ICallStackSymbol, String> cache = new HashMap<>();
        try (FlowScopeLog dfs = new FlowScopeLogBuilder(LOGGER, LEVEL, "innerSymbolUpdated#resolveNodes").setParentScope(flowScope).build()) { //$NON-NLS-1$
            for (ICallStackSymbol symbol : symbols) {
                if (!cache.containsKey(symbol)) {
                    cache.put(symbol, resolveSymbol(symbol));
                }
            }
        }

        Display.getDefault().asyncExec(() -> {
            try (FlowScopeLog dfs = new FlowScopeLogBuilder(LOGGER, LEVEL, "innerSymbolUpdated#updateNodes").setParentScope(flowScope).build()) { //$NON-NLS-1$
                for (Object value : fGraph.getNodes()) {
                    if (value instanceof GraphNode) {
                        GraphNode gn = (GraphNode) value;
                        CallGraphNode cgn = (CallGraphNode) gn.getData();
                        gn.setText(cache.getOrDefault(cgn.fSymbol, cgn.fSymbol.toString()));
                        gn.setTooltip(new Label(cgn.toString()));
                        gn.setBackgroundColor(getColor(cgn.fSymbol));
                        gn.setForegroundColor(fTextColor);
                    }
                }
                Job job = new Job(Messages.CallGraphView_LayoutJob) {
                    @Override
                    protected IStatus run(IProgressMonitor monitor) {
                        try (FlowScopeLog fsl = new FlowScopeLogBuilder(LOGGER, LEVEL, "applyGraphLayout", "blocks_ui", true).setParentScope(flowScope).build()) { //$NON-NLS-1$ //$NON-NLS-2$
                            fGraph.applyLayout();
                            return Status.OK_STATUS;
                        }
                    }
                };
                job.setSystem(true);
                job.schedule();
            }
        });

    }

    private void buildCallGraph(@NonNull FlowScopeLog flowScope) {
        if (getTrace() != null) {
            new Job(Messages.CallGraphView_WaitForParentAnalysis) {

                @Override
                protected IStatus run(IProgressMonitor monitor) {
                    try (FlowScopeLog fs = new FlowScopeLogBuilder(LOGGER, LEVEL, "buildCallGraph").setParentScope(flowScope).build()) { //$NON-NLS-1$
                        Iterable<ICallGraphProvider2> callGraphModules = getCallGraphs();
                        for (ICallGraphProvider2 module : callGraphModules) {
                            new CallGraphPopulator(Messages.CallGraphView_BuildJob, module.getCallGraph(), fs).schedule();
                        }
                        return Status.OK_STATUS;
                    }
                }
            }.schedule();
        }
    }

    private static Color getColor(ICallStackSymbol symbol) {
        return COLOR_REGISTRY.get(COLORS[(symbol.toString().hashCode() % 250 + 250 & 250)].toString());
    }

    private Set<ICallGraphProvider2> getCallGraphs() {
        ITmfTrace trace = getTrace();
        if (trace != null) {
            Iterable<ICallGraphProvider2> callgraphModules = TmfTraceUtils.getAnalysisModulesOfClass(trace, ICallGraphProvider2.class);

            return StreamSupport.stream(callgraphModules.spliterator(), false)
                    .filter(m -> {
                        if (m instanceof IAnalysisModule) {
                            return ((IAnalysisModule) m).getId().equals(fAnalysisId);
                        }
                        return true;
                    })
                    .collect(Collectors.toSet());
        }
        return Collections.emptySet();
    }

    @Override
    public void setFocus() {
        // do nothing
    }

    private ITmfTrace getTrace() {
        return fTrace;
    }

    private void setTrace(ITmfTrace trace) {
        fTrace = trace;
    }

    /**
     * Can be very slow, do not use in UI thread
     *
     * @param symbol
     *            the symbol to resolve
     * @return the resolved string
     */
    private @NonNull String resolveSymbol(ICallStackSymbol symbol) {
        ITmfTrace trace = getTrace();
        if (trace == null) {
            return String.valueOf(symbol);
        }
        return symbol.resolve(SymbolProviderManager.getInstance().getSymbolProviders(trace));
    }

    /**
     * @return the nodes
     */
    private Map<@NonNull ICallStackSymbol, GraphNode> getNodes() {
        return fNodes;
    }

    /**
     * @return the cnnx
     */
    private Collection<GraphConnection> getConnections() {
        return fCnnx;
    }

    @Override
    public AbstractZoomableViewer getZoomableViewer() {
        return fGraphViewer;
    }

}