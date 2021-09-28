package com.reiser.presto.HLL;

import net.agkn.hll.HLL;

/**
 * @author: reiserx
 * Date:2021/9/28
 * Des:
 */
public class HyperLogLogTest {

    public static void main(String[] args) {
        final HLL hll = new HLL(13/*log2m*/, 5/*registerWidth*/);
        hll.addRaw(1);
        hll.addRaw(2);
        hll.addRaw(3);
        hll.addRaw(4);
        hll.addRaw(1);
        hll.addRaw(2);
        hll.addRaw(3);
        hll.addRaw(4);
        hll.addRaw(5);

        System.out.println(hll.cardinality());
    }
}
