package org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles.constantsize;

import java.io.File;
import java.util.logging.Logger;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.tracecompass.common.core.log.TraceCompassLog;

public class HistoryTileConstantSizeConfig {

    /**
     * The magic number for this file format.
     */
    public static final int HISTORY_FILE_MAGIC_NUMBER = 0x05FFB121;
    /** Magic number, file version, provider version, header size and config size */
    private static final int STATIC_HEADER_SIZE = 4 * Integer.BYTES + Byte.BYTES + Long.BYTES;
    private static final int FILE_VERSION = 1;
    private static final @NonNull Logger LOGGER = TraceCompassLog.getLogger(HistoryTileConstantSizeConfig.class);

    private File fStateFile;
    private long fStart;
    private int fProviderVersion;

    HistoryTileConstantSizeConfig(File stateFile, int providerVersion, long startTime) {
        fStateFile = stateFile;
        fProviderVersion = providerVersion;
        fStart = startTime;
    }

    HistoryTileConstantSizeConfig(File existingStateFile, int providerVersion) {
        fStateFile = existingStateFile;
        fProviderVersion = providerVersion;
    }

    public long getStart() {
        return fStart;
    }

    public File getStateFile() {
        return fStateFile;
    }

    public int getProviderVersion() {
        return fProviderVersion;
    }

    public long getEnd() {
        // TODO: Read end from statefile
        return 0L;
    }
}
