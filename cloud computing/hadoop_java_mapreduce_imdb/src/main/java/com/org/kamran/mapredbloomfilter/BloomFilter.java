package com.org.kamran.mapredbloomfilter;

import static org.apache.hadoop.util.hash.Hash.MURMUR_HASH;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.io.Writable;
import java.io.*;
import java.util.Objects;
import java.util.BitSet;
import java.nio.charset.StandardCharsets;

public class BloomFilter implements Writable, Comparable<BloomFilter>  {
    private static final int hashType = MURMUR_HASH;
    private BitSet bitset;
    private int m;
    private int k;

    //constructors
    public BloomFilter(int m, int k){
        bitset = new BitSet(m);
        this.m = m;
        this.k = k;
    }

    public BloomFilter(){} //default constructor

    // copy constructor
    public BloomFilter(BloomFilter toBeCopies){
        this.bitset = (BitSet) toBeCopies.bitset.clone();
        this.m = toBeCopies.m;
        this.k = toBeCopies.k;
    }

    // function to check if an id exists in bloomfilter
    public boolean contains(String id){
        int seed = 0;
        for (int i = 0; i < k; i++){
            seed = Hash.getInstance(hashType).hash(id.getBytes(StandardCharsets.UTF_8), seed);
            if(!bitset.get(Math.abs(seed % m)))
                return false;
        }
        return true;
    }
    
    // function to add new members to bloomfilter
    public boolean add(String id){
        int seed = 0;
        for (int i = 0; i < k; i++){
            seed = Hash.getInstance(hashType).hash(id.getBytes(StandardCharsets.UTF_8), seed);
            bitset.set(Math.abs(seed % m));
        }
        return true;
    }

    // logical bitwise or of bitsets
    public void or(BitSet input){
        bitset.or(input);
    }

    // BitSet setter
    public void setBitset(BitSet bitset) {
        this.bitset = bitset;
    }

    // k setter
    public void setk(int k) {
        this.k = k;
    }
    // BitSet getter
    public BitSet getBitset() {
        return bitset;
    }
    // k getter
    public int getk() {
        return k;
    }

    // m getter
    public int getm() {
        return bitset.length();
    }

    @Override
    public boolean equals(Object in) {
        if (this == in)
            return true;
        if (in == null || getClass() != in.getClass()) 
            return false;
        BloomFilter that = (BloomFilter) in;
        return k == that.k &&  Objects.equals(bitset, that.bitset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bitset, k, Hash.getInstance(hashType));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //write headers
        dataOutput.writeInt(this.m);
        dataOutput.writeInt(this.k);
        long[] longs = bitset.toLongArray();
        dataOutput.writeInt(longs.length);
        for (int i = 0; i < longs.length; i++) {
            dataOutput.writeLong(longs[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //write headers
        m = dataInput.readInt();
        k = dataInput.readInt();
        long[] longs = new long[dataInput.readInt()];
        for (int i = 0; i < longs.length; i++) {
            longs[i] = dataInput.readLong();
        }
        bitset = BitSet.valueOf(longs);
    }

    @Override
    public String toString() {
        return bitset.toString();
    }

    @Override
    public int compareTo(BloomFilter bf) {
        if (bf.equals(bitset))
            return 1;
        return 0;
    }
}
