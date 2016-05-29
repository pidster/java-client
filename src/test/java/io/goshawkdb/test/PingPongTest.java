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
import java.util.Queue;

import io.goshawkdb.client.Connection;
import io.goshawkdb.client.GoshawkObj;
import io.goshawkdb.client.Transaction;
import io.goshawkdb.client.TxnId;

public class PingPongTest extends TestBase {

    public PingPongTest() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
    }

    @Test
    public void pingPong() throws Exception {
        try {
            int limit = 1000;
            int threadCount = 4;
            TxnId origRootVsn = setRootToZeroInt64(createConnections(1)[0]);

            inParallel(threadCount, (int tId, Connection c, Queue<Exception> exceptionQ) -> {
                awaitRootVersionChange(c, origRootVsn);
                boolean inProgress = true;
                while (inProgress) {
                    inProgress = c.runTransaction(txn -> {
                        GoshawkObj root = txn.getRoot();
                        ByteBuffer valBuf = root.getValue().order(ByteOrder.BIG_ENDIAN);
                        long val = valBuf.getLong(0);
                        if (val > limit) {
                            return false;
                        } else if (val % threadCount == tId) {
                            System.out.println("" + tId + " incrementing at " + val);
                            root.set(valBuf.putLong(0, val + 1));
                        } else {
                            txn.retry();
                            throw new IllegalStateException("Reached unreachable code!");
                        }
                        return true;
                    }).result;
                }
            });
        } finally {
            shutdown();
        }
    }
}
