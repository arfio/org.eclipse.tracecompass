package org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles;

import java.io.File;
import java.io.IOException;

import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.tracecompass.statesystem.core.backend.IStateHistoryBackend;

/**
 * @since 5.4
 *
 */
@NonNullByDefault
public class HistoryTileBackendFactory {

    private static final int N_PIXELS = 2000;
    private static final int REDUCTION_FACTOR = 25;
    private static final long MIN_RESOLUTION = 10000;

    private HistoryTileBackendFactory() {}

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
            File stateFile, int providerVersion, long startTime, int nPixels, long[] resolutions) throws IOException {
        return new HistoryTileBackend(ssid, stateFile, providerVersion, startTime, nPixels, resolutions);
    }

    public static IStateHistoryBackend createHistoryTreeBackendNewFile(String ssid, long startTime, long endTime, int providerVersion, File stateFile) throws IOException {
        long largestResolution = (endTime - startTime) / N_PIXELS + 1;
        long resolution = largestResolution;
        int nTiles = (int) Math.round(Math.log((double) MIN_RESOLUTION / largestResolution) / Math.log(REDUCTION_FACTOR / 100.0)) + 1;
        long[] resolutions = new long[nTiles];
        for (int i = 0; i < nTiles; i++) {
            resolutions[i] = resolution;
            resolution *= REDUCTION_FACTOR / 100.0;
        }
        return new HistoryTileBackend(ssid, stateFile, providerVersion, startTime, N_PIXELS, resolutions);
    }

    public static IStateHistoryBackend createHistoryTreeBackendExistingFile(String ssid, int providerVersion, File stateFile) throws IOException {
        return new HistoryTileBackend(ssid, stateFile, providerVersion);
    }
}
