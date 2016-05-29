package io.goshawkdb.test;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObj;
import io.goshawkdb.client.Transaction;

public class SoloCountTest extends TestBase {

    public SoloCountTest() throws CertificateException, InvalidKeySpecException, NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException, InvalidKeyException, IOException {
        super();
    }

    @Test
    public void soloCount() throws Exception {
        try {
            Connection conn = createConnections(1)[0];
            setRootToZeroInt64(conn);
            long start = System.nanoTime();
            long expected = 0L;
            for (int idx = 0; idx < 1000; idx++) {
                long expectedCopy = expected;
                expected = conn.runTransaction(txn -> {
                    GoshawkObj root = txn.getRoot();
                    ByteBuffer valBuf = root.getValue().order(ByteOrder.BIG_ENDIAN);
                    long old = valBuf.getLong(0);
                    if (old == expectedCopy) {
                        long val = old + 1;
                        root.set(valBuf.putLong(0, val));
                        return val;
                    } else {
                        throw new IllegalStateException("Expected " + expectedCopy + " but found " + old);
                    }
                }).result;
            }
            long end = System.nanoTime();
            System.out.println("Elapsed time: " + ((double) (end - start)) / 1000000D + "ms");
        } finally {
            shutdown();
        }
    }
}
