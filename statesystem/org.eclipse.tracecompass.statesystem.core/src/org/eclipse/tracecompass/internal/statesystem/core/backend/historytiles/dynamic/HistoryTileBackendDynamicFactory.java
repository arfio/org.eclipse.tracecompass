package org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles.dynamic;

import java.io.File;
import java.io.IOException;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.tracecompass.statesystem.core.backend.IStateHistoryBackend;

/**
 * @since 5.4
 *
 */
@NonNullByDefault
public class HistoryTileBackendDynamicFactory {

    private static final long MIN_RESOLUTION = 10000;
    private static final int N_PIXELS = 2000;

    private HistoryTileBackendDynamicFactory() {}

    /**
     * Create a new backend using a History Tree. This backend stores all its
     * intervals on disk.
     *
     * By specifying a 'queueSize' parameter, the implementation that runs in a
     * separate thread can be used.
     *
     * @param ssid
     *            The state system's id
     * @param stateFile
     *            The filename/location where to store the state history (Should
     *            end in .ht)
     * @param providerVersion
     *            Version of of the state provider. We will only try to reopen
     *            existing files if this version matches the one in the
     *            framework.
     * @param startTime
     *            The earliest time stamp that will be stored in the history
     * @return The state system backend
     * @throws IOException
     */
    public static IStateHistoryBackend createHistoryTreeBackendNewFile(String ssid,
            File stateFile, int providerVersion, long startTime, int nPixels, boolean isEveryIntervalContiguous) throws IOException {
        return new HistoryTileBackendDynamic(ssid, stateFile, providerVersion, startTime, MIN_RESOLUTION, nPixels, isEveryIntervalContiguous);
    }

    public static IStateHistoryBackend createHistoryTreeBackendNewFile(String ssid, long startTime, int providerVersion, File stateFile, boolean isEveryIntervalContiguous) throws IOException {
        return new HistoryTileBackendDynamic(ssid, stateFile, providerVersion, startTime, MIN_RESOLUTION, N_PIXELS, isEveryIntervalContiguous);
    }

    public static IStateHistoryBackend createHistoryTreeBackendExistingFile(String ssid, int providerVersion, File stateFile) throws IOException {
        return new HistoryTileBackendDynamic(ssid, stateFile, providerVersion);
    }
}
