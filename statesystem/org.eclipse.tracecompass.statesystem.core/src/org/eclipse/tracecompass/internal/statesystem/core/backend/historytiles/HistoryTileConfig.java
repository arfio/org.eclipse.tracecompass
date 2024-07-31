package org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.tracecompass.common.core.log.TraceCompassLog;
import org.eclipse.tracecompass.common.core.log.TraceCompassLogUtils.FlowScopeLog;
import org.eclipse.tracecompass.common.core.log.TraceCompassLogUtils.FlowScopeLogBuilder;
import org.eclipse.tracecompass.statesystem.core.ITmfStateSystemBuilder;

/**
 * @since 5.4
 *
 */
public class HistoryTileConfig {

    /**
     * The magic number for this file format.
     */
    public static final int HISTORY_FILE_MAGIC_NUMBER = 0x05FFB100;
    /** Magic number, file version, provider version and header size */
    private static final int STATIC_HEADER_SIZE = 4 * Integer.BYTES;
    private static final int FILE_VERSION = 1;
    private static final @NonNull Logger LOGGER = TraceCompassLog.getLogger(HistoryTileConfig.class);
    private long[] fResolutions;
    private File fStateFile;
    private long fStart;
    private int fProviderVersion;
    private int fNPixels;

    private List<long[]> fTilePositions;

    HistoryTileConfig(File stateFile, int providerVersion, long startTime, int nPixels, long[] resolutions) {
        fStateFile = stateFile;
        fProviderVersion = providerVersion;
        fStart = startTime;
        fNPixels = nPixels;
        fResolutions = resolutions;
        fTilePositions = new ArrayList<>();
        for (int i = 0; i < fResolutions.length; i++) {
            fTilePositions.add(new long[calculateNumberOfTiles(i)]);
        }
    }

    HistoryTileConfig(File existingStateFile, int providerVersion) {
        fStateFile = existingStateFile;
        fProviderVersion = providerVersion;
    }

    public int calculateNumberOfTiles(int resolutionIndex) {
        if (fResolutions.length > 0) {
            return (int) Math.ceil((fResolutions[0] * fNPixels) / (double)(fResolutions[resolutionIndex] * fNPixels));
        }
        return 0;
    }

    public long getStartTileSection() {
        return calculateConfigHeaderSize() + STATIC_HEADER_SIZE;
    }

    public void addTile(HistoryTile tile, long tilePosition) {
        long start = tile.getStart();
        int tileIndex = (int) ((start - fStart) / (tile.getResolution() * fNPixels));
        for (int i = 0; i < fResolutions.length; i++) {
            if (fResolutions[i] == tile.getResolution()) {
                fTilePositions.get(i)[tileIndex] = tilePosition;
            }
        }
    }

    public HistoryTile readTile(FileChannel channel, int resolutionIndex, int tileIndex) {
        try (FlowScopeLog next = new FlowScopeLogBuilder(LOGGER, Level.FINER,
                "HistoryTileConfig:readTile").build()) { //$NON-NLS-1$
            long start = fStart + fResolutions[resolutionIndex] * fNPixels * tileIndex;
            long end = start + fResolutions[resolutionIndex] * fNPixels;
            long tilePosition = fTilePositions.get(resolutionIndex)[tileIndex];
            if (tilePosition == 0) {
                return new HistoryTile(fResolutions[resolutionIndex], start, end);
            }
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.clear();
            try {
                channel.position(tilePosition);
                channel.read(buffer);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            buffer.flip();
            int tileSize = buffer.getInt();
            return HistoryTile.readTile(channel, tilePosition, tileSize, fResolutions[resolutionIndex], start, end);
        }
    }

    public void writeHeader(FileChannel channel) {
        int configHeaderSize = calculateConfigHeaderSize();
        int headerSize = configHeaderSize + STATIC_HEADER_SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(headerSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.clear();

        /* Save the config of the tree to the header of the file */
        try {
            channel.position(0);

            /* Static header portion */
            buffer.putInt(HISTORY_FILE_MAGIC_NUMBER);
            buffer.putInt(FILE_VERSION);
            buffer.putInt(fProviderVersion);
            buffer.putInt(configHeaderSize);

            /* Config header portion */
            buffer.putInt(fNPixels);
            buffer.putInt(fResolutions.length);

            if (fResolutions.length > 0) {
                for (int i = 0; i < fResolutions.length; i++) {
                    buffer.putLong(fResolutions[i]);
                    buffer.putInt(fTilePositions.get(i).length);
                    for (int j = 0; j < fTilePositions.get(i).length; j++) {
                        buffer.putLong(fTilePositions.get(i)[j]);
                    }
                }
            }

            buffer.flip();
            int res = channel.write(buffer);

            if (res > headerSize) {
                throw new IOException("Tree header size = " + headerSize + " but wrote " + res); //$NON-NLS-1$ //$NON-NLS-2$
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void readHeader(FileChannel channel) {
        ByteBuffer staticHeaderBuffer = ByteBuffer.allocate(STATIC_HEADER_SIZE);
        staticHeaderBuffer.order(ByteOrder.LITTLE_ENDIAN);
        staticHeaderBuffer.clear();

        try {
            channel.position(0);
            /* Static header portion */
            channel.read(staticHeaderBuffer);

            int magicNumber = staticHeaderBuffer.getInt();
            if (magicNumber != HISTORY_FILE_MAGIC_NUMBER) {
                throw new IOException("Wrong magic number"); //$NON-NLS-1$
            }
            int fileVersion = staticHeaderBuffer.getInt();
            if (fileVersion != FILE_VERSION) {
                throw new IOException("Mismatching History Tile file format versions"); //$NON-NLS-1$
            }
            int providerVersion = staticHeaderBuffer.getInt();
            if (providerVersion != fProviderVersion && providerVersion != ITmfStateSystemBuilder.IGNORE_PROVIDER_VERSION) {
                throw new IOException("Mismatching event handler versions"); //$NON-NLS-1$
            }
            int configHeaderSize = staticHeaderBuffer.getInt();

            /* Config header portion */
            ByteBuffer configHeaderBuffer = ByteBuffer.allocate(configHeaderSize);
            configHeaderBuffer.order(ByteOrder.LITTLE_ENDIAN);
            configHeaderBuffer.clear();
            channel.read(configHeaderBuffer);

            fNPixels = configHeaderBuffer.getInt();
            int nResolutions = configHeaderBuffer.getInt();
            fResolutions = new long[nResolutions];

            if (fResolutions.length > 0) {
                for (int i = 0; i < fResolutions.length; i++) {
                    fResolutions[i] = configHeaderBuffer.getLong();
                    configHeaderBuffer.getInt(fTilePositions.get(i).length);
                    for (int j = 0; j < fTilePositions.get(i).length; j++) {
                        configHeaderBuffer.putLong(fTilePositions.get(i)[j]);
                    }
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private int calculateConfigHeaderSize() {
        /* number of pixels + number of resolutions */
        int headerSize = Integer.BYTES * 2;
        /* number of resolutions * (resolution + number of tiles) */
        headerSize += (Long.BYTES + Integer.BYTES) * fResolutions.length;
        /* number of tiles * tile start position */
        for (int i = 0; i < fResolutions.length; i++) {
            headerSize += calculateNumberOfTiles(i) * Long.BYTES;
        }
        return headerSize;
    }

    public int getNPixels() {
        return fNPixels;
    }

    public long[] getResolutions() {
        return fResolutions;
    }

    public long getStart() {
        return fStart;
    }

    public long getEnd() {
        if (fResolutions.length > 0) {
            return fStart + fResolutions[0] * fNPixels;
        }
        return 0L;
    }

    public File getStateFile() {
        return fStateFile;
    }

    public int getProviderVersion() {
        return fProviderVersion;
    }
}
