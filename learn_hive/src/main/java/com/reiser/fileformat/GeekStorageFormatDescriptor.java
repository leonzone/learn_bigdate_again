package com.reiser.fileformat;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.ql.io.AbstractStorageFormatDescriptor;

import java.util.Set;

/**
 * @author: reiserx
 * Date:2021/8/12
 * Des:
 */
public class GeekStorageFormatDescriptor  extends AbstractStorageFormatDescriptor {
    @Override
    public Set<String> getNames() {
        return ImmutableSet.of("geek");
    }

    @Override
    public String getInputFormat() {
        return GeekTextInputFormat.class.getName();
    }

    @Override
    public String getOutputFormat() {
        return GeekTextOutputFormat.class.getName();
    }
}
