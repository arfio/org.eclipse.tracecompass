package org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles.constantsize;

import java.io.File;
import java.io.IOException;

import org.eclipse.tracecompass.statesystem.core.backend.IStateHistoryBackend;

public class HistoryTileConstantSizeBackendFactory {

    private static final int TILE_SIZE = 256 * 1024;

    private HistoryTileConstantSizeBackendFactory() {}

    public static IStateHistoryBackend createHistoryBackendNewFile(String ssid, int providerVersion, File stateFile, long startTime) throws IOException {
        return new HistoryTileConstantSizeBackend(ssid, providerVersion, stateFile, startTime, TILE_SIZE);
    }

    public static IStateHistoryBackend createHistoryBackendExistingFile(String ssid, int providerVersion, File stateFile) throws IOException {
        return new HistoryTileConstantSizeBackend(ssid, providerVersion, stateFile);
    }
}
