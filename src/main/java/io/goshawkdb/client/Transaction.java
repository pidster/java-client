package io.goshawkdb.client;

import java.nio.ByteBuffer;

/**
 * @author pidster
 */
public interface Transaction {

    /**
     *
     */
    void retry();

    /**
     * @return obj
     */
    GoshawkObj getRoot();

    /**
     * @param value to create
     * @param references to apply
     * @return obj
     */
    GoshawkObj createObject(ByteBuffer value, GoshawkObj... references);

    /**
     * @param vUUId of object
     * @return obj
     */
    GoshawkObj getObject(VarUUId vUUId);

}
