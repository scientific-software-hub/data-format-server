package org.waltz.nexus;

import ncsa.hdf.object.h5.H5File;

public interface NxdlElement {
    void create(H5File h5file) throws Exception;
}
