package org.eclipse.tracecompass.internal.statesystem.core.backend.historytiles.dynamic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.tracecompass.datastore.core.encoding.HTVarInt;
import org.eclipse.tracecompass.datastore.core.serialization.ISafeByteBufferReader;
import org.eclipse.tracecompass.datastore.core.serialization.ISafeByteBufferWriter;
import org.eclipse.tracecompass.datastore.core.serialization.SafeByteBufferFactory;
import org.eclipse.tracecompass.internal.provisional.statesystem.core.statevalue.CustomStateValue;
import org.eclipse.tracecompass.statesystem.core.exceptions.TimeRangeException;
import org.eclipse.tracecompass.statesystem.core.interval.ITmfStateInterval;
import org.eclipse.tracecompass.statesystem.core.statevalue.ITmfStateValue;
import org.eclipse.tracecompass.statesystem.core.statevalue.TmfStateValue;

/**
 * @since 5.4
 *
 */
public class TileInterval implements ITmfStateInterval {

    private int fAttribute;
    private long fStartTime;
    private long fEndTime;
    private Object fValue;

    private static final Charset CHARSET = StandardCharsets.UTF_8;
    /* 'Byte' equivalent for state values types */
    private static final byte TYPE_NULL = -1;
    private static final byte TYPE_INTEGER = 0;
    private static final byte TYPE_STRING = 1;
    private static final byte TYPE_LONG = 2;
    private static final byte TYPE_DOUBLE = 3;
    private static final byte TYPE_CUSTOM = 20;

    TileInterval(long start, long end, int attribute, Object value) {
        fStartTime = start;
        fEndTime = end;
        fAttribute = attribute;
        fValue = value;
    }

    @Override
    public long getStartTime() {
        return fStartTime;
    }

    @Override
    public long getEndTime() {
        return fEndTime;
    }

    public void setEndTime(long endTime) {
        fEndTime = endTime;
    }

    @Override
    public int getAttribute() {
        return fAttribute;
    }

    @Override
    public @NonNull ITmfStateValue getStateValue() {
        return TmfStateValue.newValue(fValue);
    }

    public boolean isNull() {
        return fValue == null;
    }

    @Override
    public boolean intersects(long timestamp) {
        return timestamp >= fStartTime && timestamp <= fEndTime;
    }

    public int getSizeOnDisk(boolean isEveryIntervalContiguous) {
        /*
         * Size is duration + type + state and if not contiguous, start has to
         * be added.
         */
        int size = HTVarInt.getEncodedLengthLong(fEndTime - fStartTime) + Byte.BYTES + getStateSize();
        if (!isEveryIntervalContiguous) {
            size += HTVarInt.getEncodedLengthLong(fStartTime);
        }
        return size;
    }

    private int getStateSize() {
        if (fValue == null) {
            return 0;
        } else if (fValue instanceof Integer) {
            return Integer.BYTES;
        } else if (fValue instanceof Long) {
            return Long.BYTES;
        } else if (fValue instanceof Double) {
            return Double.BYTES;
        } else if (fValue instanceof CustomStateValue customStateValue) {
            /* Length of serialized value (short) + state value */
            return Short.BYTES + customStateValue.getSerializedSize();
        }
        String str = String.valueOf(fValue);
        int strLength = str.getBytes(CHARSET).length;

        if (strLength > Short.MAX_VALUE) {
            throw new IllegalArgumentException("String is too long to be stored in state system: " + str); //$NON-NLS-1$
        }
        /*
         * String's length + 3 (2 bytes for size, 1 byte for \0 at the end)
         */
        return strLength + 3;
    }

    public void writeInterval(ByteBuffer buffer, boolean isEveryIntervalContiguous) {
        if (fValue != null) {
            @NonNull
            Object value = fValue;
            if (value instanceof Integer) {
                buffer.put(TYPE_INTEGER);
                buffer.putInt((int) value);
            } else if (value instanceof Long) {
                buffer.put(TYPE_LONG);
                buffer.putLong((long) value);
            } else if (value instanceof Double) {
                buffer.put(TYPE_DOUBLE);
                buffer.putDouble((double) value);
            } else if (value instanceof CustomStateValue customStateValue) {
                buffer.put(TYPE_CUSTOM);
                int size = customStateValue.getSerializedSize();
                buffer.putShort((short) size);
                ISafeByteBufferWriter safeBuffer = SafeByteBufferFactory.wrapWriter(buffer, size);
                customStateValue.serialize(safeBuffer);
            } else {
                String string = String.valueOf(value);
                buffer.put(TYPE_STRING);
                byte[] strArray = string.getBytes(CHARSET);

                /*
                 * Write the Strings entry (1st byte = size, then the bytes,
                 * then the 0). We have checked the string length at the
                 * constructor.
                 */
                buffer.putShort((short) strArray.length);
                buffer.put(strArray);
                buffer.put((byte) 0);
            }
        } else {
            buffer.put(TYPE_NULL);
        }
        if (!isEveryIntervalContiguous) {
            HTVarInt.writeLong(buffer, fStartTime);
        }
        HTVarInt.writeLong(buffer, fEndTime - fStartTime);
    }

    /**
     * This method reads an interval from the provided buffer.
     *
     * The start time is read directly from the buffer as this method is
     * expected to be used on non contiguous intervals
     *
     * @param buffer
     *            the buffer to read from
     * @param attribute
     *            the attribute of the interval
     * @return the interval
     * @throws IOException
     *             value or time are incorrect or the buffer was not correctly
     *             allocated
     */
    public static TileInterval readInterval(ByteBuffer buffer, int attribute) throws IOException {
        Object value = readState(buffer);
        try {
            long intervalStart = HTVarInt.readLong(buffer);
            long intervalEnd = HTVarInt.readLong(buffer) + intervalStart;
            return new TileInterval(intervalStart, intervalEnd, attribute, value);
        } catch (TimeRangeException e) {
            throw new IOException();
        }
    }

    public static TileInterval readInterval(ByteBuffer buffer, long intervalStart, int attribute) throws IOException {
        Object value = readState(buffer);
        try {
            long intervalEnd = HTVarInt.readLong(buffer) + intervalStart;
            return new TileInterval(intervalStart, intervalEnd, attribute, value);
        } catch (TimeRangeException e) {
            throw new IOException();
        }
    }

    private static Object readState(ByteBuffer buffer) throws IOException {
        Object value;
        /* Read the 'type' of the value, then react accordingly */
        byte valueType = buffer.get();
        switch (valueType) {
        case TYPE_NULL:
            value = null;
            break;

        case TYPE_INTEGER:
            value = buffer.getInt();
            break;

        case TYPE_STRING: {
            /* the first short = the size to read */
            int valueSize = buffer.getShort();

            byte[] array = new byte[valueSize];
            buffer.get(array);
            value = new String(array, CHARSET);

            /* Confirm the 0'ed byte at the end */
            byte res = buffer.get();
            if (res != 0) {
                throw new IOException();
            }
            break;
        }

        case TYPE_LONG:
            /* Go read the matching entry in the Strings section of the block */
            value = buffer.getLong();
            break;

        case TYPE_DOUBLE:
            /* Go read the matching entry in the Strings section of the block */
            value = buffer.getDouble();
            break;

        case TYPE_CUSTOM: {
            short valueSize = buffer.getShort();
            ISafeByteBufferReader safeBuffer = SafeByteBufferFactory.wrapReader(buffer, valueSize);
            value = CustomStateValue.readSerializedValue(safeBuffer);
            break;
        }
        default:
            /* Unknown data, better to not make anything up... */
            throw new IOException();
        }
        return value;
    }
}
