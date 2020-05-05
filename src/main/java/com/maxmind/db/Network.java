package com.maxmind.db;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * <code>Network</code> represents an IP network.
 */
public abstract class Network {
    private final int prefixLength;

    /**
     * Construct a <code>Network</code>
     *
     * @param prefixLength The prefix length for the network.
     */
    protected Network(int prefixLength) {
        this.prefixLength = prefixLength;
    }

    public static Network forAddress(int address, int prefixLength) { return new NetworkV4(address, prefixLength); }
    public static Network forAddress(long high, long low, int prefixLength) { return new NetworkV6(high, low, prefixLength); }
    public static Network forInetAddress(InetAddress address, int prefixLength) {
        ByteBuffer bb = ByteBuffer.wrap(address.getAddress()).order(ByteOrder.BIG_ENDIAN);
        if (bb.remaining() == 4) {
            return forAddress( bb.getInt(), prefixLength );
        } else if (bb.remaining() == 16) {
            long high = bb.getLong();
            long low = bb.getLong();
            return forAddress(high, low, prefixLength);
        } else {
            throw new InternalError("address type not supported: "+address);
        }
    }

    /**
     * @return The first address in the network.
     */
    public abstract InetAddress getNetworkAddress();

    /**
     * @return The prefix length is the number of leading 1 bits in the subnet
     * mask. Sometimes also known as netmask length.
     */
    public int getPrefixLength() {
        return prefixLength;
    }

    /***
     * @return A string representation of the network in CIDR notation, e.g.,
     * <code>1.2.3.0/24</code> or <code>2001::/8</code>.
     */
    public String toString() {
        return getNetworkAddress().getHostAddress() + "/" + prefixLength;
    }
}

class NetworkV4 extends Network {
    final int address;
    protected InetAddress networkAddress = null;

    NetworkV4(int address, int prefixLength) {
        super(prefixLength);
        this.address = address;
    }

    @Override
    public InetAddress getNetworkAddress() {

        if (networkAddress != null)
            return networkAddress;

        byte[] data = new byte[4];

        int addr;

        if (getPrefixLength() == 32) {
            addr = address;
        } else if (getPrefixLength() == 0) {
            addr = 0;
        } else {
            addr = address & (0xffffffff << (32 - getPrefixLength()));
        }

        data[0] = (byte) ((addr >> 24) & 0xff);
        data[1] = (byte) ((addr >> 16) & 0xff);
        data[2] = (byte) ((addr >> 8) & 0xff);
        data[3] = (byte) ((addr) & 0xff);

        try {
            networkAddress = InetAddress.getByAddress(data);
        } catch (UnknownHostException e) {
            throw new InternalError("implementation error", e);
        }

        return networkAddress;
    }
}

class NetworkV6 extends Network {
    final long lowAddress;
    final long highAddress;
    protected InetAddress networkAddress = null;

    NetworkV6(long highAddress, long lowAddress, int prefixLength) {
        super(prefixLength);
        this.highAddress = highAddress;
        this.lowAddress = lowAddress;
    }

    @Override
    public InetAddress getNetworkAddress() {
        if (networkAddress != null)
            return networkAddress;

        byte[] data = new byte[16];

        long high;

        if (getPrefixLength() >= 64)
            high = highAddress;
        else if (getPrefixLength() == 0)
            high = 0;
        else
            high = highAddress & (0xffffffffffffffffL << (64-Math.min(64, getPrefixLength())));

        data[0] = (byte) ((high >> 56) & 0xff);
        data[1] = (byte) ((high >> 48) & 0xff);
        data[2] = (byte) ((high >> 40) & 0xff);
        data[3] = (byte) ((high >> 32) & 0xff);
        data[4] = (byte) ((high >> 24) & 0xff);
        data[5] = (byte) ((high >> 16) & 0xff);
        data[6] = (byte) ((high >> 8) & 0xff);
        data[7] = (byte) ((high) & 0xff);

        long low;

        if (getPrefixLength() <= 64) {
            low = 0;
        } else {
            low = lowAddress & (0xffffffffffffffffL << (64 - (getPrefixLength()-64)));
        }

        data[8] = (byte) ((low >> 56) & 0xff);
        data[9] = (byte) ((low >> 48) & 0xff);
        data[10] = (byte) ((low >> 40) & 0xff);
        data[11] = (byte) ((low >> 32) & 0xff);
        data[12] = (byte) ((low >> 24) & 0xff);
        data[13] = (byte) ((low >> 16) & 0xff);
        data[14] = (byte) ((low >> 8) & 0xff);
        data[15] = (byte) ((low) & 0xff);

        try {
            networkAddress = InetAddress.getByAddress(data);
        } catch (UnknownHostException e) {
            throw new InternalError("implementation error", e);
        }

        return networkAddress;
    }
}