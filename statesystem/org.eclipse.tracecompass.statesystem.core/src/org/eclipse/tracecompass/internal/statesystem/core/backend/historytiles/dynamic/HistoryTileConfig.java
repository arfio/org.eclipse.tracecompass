package org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles.dynamic;

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
import org.eclipse.tracecompass.traceeventlogger.LogUtils.FlowScopeLog;
import org.eclipse.tracecompass.traceeventlogger.LogUtils.FlowScopeLogBuilder;
import org.eclipse.tracecompass.statesystem.core.ITmfStateSystemBuilder;

/**
 * @since 5.4
 *
 */
public class HistoryTileConfig {

    /**
     * The magic number for this file format.
     */
    public static final int HISTORY_FILE_MAGIC_NUMBER = 0x05FFB101;
    /** Magic number, file version, provider version, header size and config size */
    private static final int STATIC_HEADER_SIZE = 4 * Integer.BYTES + Byte.BYTES + Long.BYTES;
    private static final int FILE_VERSION = 1;
    private static final @NonNull Logger LOGGER = TraceCompassLog.getLogger(HistoryTileConfig.class);
    /**
     * We expect every interval to have an end time one nanosecond less than the
     * next interval start time if true.
     */
    private boolean fIsEveryIntervalContiguous;
    private List<Long> fResolutions;
    private File fStateFile;
    private long fStart;
    private int fProviderVersion;
    private int fNPixels;

    private List<long[]> fTilePositions;

    HistoryTileConfig(File stateFile, int providerVersion, long startTime, int nPixels, boolean isEveryIntervalContiguous) {
        fIsEveryIntervalContiguous = isEveryIntervalContiguous;
        fStateFile = stateFile;
        fProviderVersion = providerVersion;
        fStart = startTime;
        fNPixels = nPixels;
        fResolutions = new ArrayList<>();
        fTilePositions = new ArrayList<>();
    }

    HistoryTileConfig(File existingStateFile, int providerVersion) {
        fStateFile = existingStateFile;
        fProviderVersion = providerVersion;
    }

    public int calculateNumberOfTiles(int resolutionIndex) {
        if (!fResolutions.isEmpty()) {
            return (int) Math.ceil((fResolutions.get(fResolutions.size() - 1) * fNPixels) / (double) (fResolutions.get(resolutionIndex) * fNPixels));
        }
        return 0;
    }

    public long getStartTileSection() {
        return STATIC_HEADER_SIZE;
    }

    public long getStartTreeSection(FileChannel channel) {
        return getStartConfigSection(channel) + calculateConfigHeaderSize();
    }

    public long getStartConfigSection(FileChannel channel) {
        long lastTilePosition = 0;
        for (long[] tilePositions: fTilePositions) {
            lastTilePosition = Long.max(tilePositions[tilePositions.length - 1], lastTilePosition);
        }
        return lastTilePosition + getTileSize(channel, lastTilePosition);
    }

    public void addTile(HistoryTile tile, long tilePosition) {
        long start = tile.getStart();
        int tileIndex = (int) ((start - fStart) / (tile.getResolution() * fNPixels));
        for (int i = 0; i < fResolutions.size(); i++) {
            if (fResolutions.get(i) == tile.getResolution()) {
                fTilePositions.get(i)[tileIndex] = tilePosition;
            }
        }
    }

    public HistoryTile readTile(FileChannel channel, int resolutionIndex, int tileIndex) {
        try (FlowScopeLog next = new FlowScopeLogBuilder(LOGGER, Level.FINER,
                "HistoryTileConfig:readTile").build()) { //$NON-NLS-1$
            long start = fStart + fResolutions.get(resolutionIndex) * fNPixels * tileIndex;
            long end = start + fResolutions.get(resolutionIndex) * fNPixels;
            long tilePosition = fTilePositions.get(resolutionIndex)[tileIndex];

            if (tilePosition == 0) {
                return new HistoryTile(fResolutions.get(resolutionIndex), start, end);
            }
            int tileSize = getTileSize(channel, tilePosition);
            ByteBuffer buffer = ByteBuffer.allocate(tileSize);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.clear();
            try {
                synchronized(this) {
                    channel.position(tilePosition);
                    channel.read(buffer);
                }
                return HistoryTile.readTile(buffer, fResolutions.get(resolutionIndex), start, end, fIsEveryIntervalContiguous);
            } catch(IOException e) {

            }
            return new HistoryTile(fResolutions.get(resolutionIndex), start, end);
        }
    }

    private int getTileSize(FileChannel channel, long tilePosition) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.clear();
        try {
            synchronized(this) {
                channel.position(tilePosition);
                channel.read(buffer);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        buffer.flip();
        return buffer.getInt();
    }

    public void writeConfig(FileChannel channel) {
        int staticHeaderSize = STATIC_HEADER_SIZE;
        ByteBuffer staticHeaderBuffer = ByteBuffer.allocate(staticHeaderSize);
        staticHeaderBuffer.order(ByteOrder.LITTLE_ENDIAN);
        staticHeaderBuffer.clear();
        int configSize = calculateConfigHeaderSize();
        ByteBuffer configBuffer = ByteBuffer.allocate(configSize);
        configBuffer.order(ByteOrder.LITTLE_ENDIAN);
        configBuffer.clear();
        long configPosition = getStartConfigSection(channel);
        /* Save the config of the tree to the header of the file */
        try {
            channel.position(0);

            /* Static header portion */
            staticHeaderBuffer.putInt(HISTORY_FILE_MAGIC_NUMBER);
            staticHeaderBuffer.putInt(FILE_VERSION);
            staticHeaderBuffer.putInt(fProviderVersion);
            staticHeaderBuffer.putInt(configSize);
            staticHeaderBuffer.putLong(configPosition);
            staticHeaderBuffer.put((byte) (fIsEveryIntervalContiguous ? 1 : 0));

            staticHeaderBuffer.flip();
            channel.write(staticHeaderBuffer);

            /* Config header portion */
            configBuffer.putInt(fNPixels);
            configBuffer.putInt(fResolutions.size());

            if (fResolutions.size() > 0) {
                for (int i = 0; i < fResolutions.size(); i++) {
                    configBuffer.putLong(fResolutions.get(i));
                    configBuffer.putInt(fTilePositions.get(i).length);
                    for (int j = 0; j < fTilePositions.get(i).length; j++) {
                        configBuffer.putLong(fTilePositions.get(i)[j]);
                    }
                }
            }
            configBuffer.flip();
            channel.position(configPosition);
            int res = channel.write(configBuffer);

            if (res > configSize) {
                throw new IOException("Config header size = " + configBuffer + " but wrote " + res); //$NON-NLS-1$ //$NON-NLS-2$
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public long readHeader(FileChannel channel) throws IOException {
        ByteBuffer staticHeaderBuffer = ByteBuffer.allocate(STATIC_HEADER_SIZE);
        staticHeaderBuffer.order(ByteOrder.LITTLE_ENDIAN);
        staticHeaderBuffer.clear();

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
        int configSize = staticHeaderBuffer.getInt();
        long configPosition = staticHeaderBuffer.getLong();
        fIsEveryIntervalContiguous = staticHeaderBuffer.get() == 1;

        /* Config header portion */
        ByteBuffer configHeaderBuffer = ByteBuffer.allocate(configSize);
        configHeaderBuffer.order(ByteOrder.LITTLE_ENDIAN);
        configHeaderBuffer.clear();
        channel.position(configPosition);
        channel.read(configHeaderBuffer);

        fNPixels = configHeaderBuffer.getInt();
        int nResolutions = configHeaderBuffer.getInt();
        fResolutions = new ArrayList<>(nResolutions);

        return configPosition;
    }

    private int calculateConfigHeaderSize() {
        /* number of pixels + number of resolutions */
        int headerSize = Integer.BYTES * 2;
        /* number of resolutions * (resolution + number of tiles) */
        headerSize += (Long.BYTES + Integer.BYTES) * fResolutions.size();
        /* number of tiles * tile start position */
        for (int i = 0; i < fResolutions.size(); i++) {
            headerSize += calculateNumberOfTiles(i) * Long.BYTES;
        }
        return headerSize;
    }

    public int getNPixels() {
        return fNPixels;
    }

    public List<Long> getResolutions() {
        return fResolutions;
    }

    public long getStart() {
        return fStart;
    }

    public boolean isEveryIntervalContiguous() {
        return fIsEveryIntervalContiguous;
    }

    public long getEnd() {
        if (fResolutions.size() > 0) {
            return fStart + fResolutions.get(0) * fNPixels;
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
