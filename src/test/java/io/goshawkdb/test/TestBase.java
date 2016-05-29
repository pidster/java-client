package io.goshawkdb.test;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

import io.goshawkdb.client.Certs;
import io.goshawkdb.client.Connection;
import io.goshawkdb.client.ConnectionFactory;
import io.goshawkdb.client.GoshawkObj;
import io.goshawkdb.client.Transaction;
import io.goshawkdb.client.TxnId;

public class TestBase {

    public interface ParRunner {
        void run(int parIndex, Connection conn, Queue<Exception> exceptionQ) throws Exception;
    }

    private ConnectionFactory factory;
    private Certs certs;
    private String[] hosts;
    private List<Connection> connections = new ArrayList<>();

    TestBase() throws NoSuchProviderException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, InvalidKeyException {
        String clusterCertPath = getEnv("CLUSTER_CERT");
        String clientKeyPairPath = getEnv("CLIENT_KEYPAIR");

        certs = new Certs();
        certs.addClusterCertificate("goshawkdb", new FileInputStream(clusterCertPath));
        certs.parseClientPEM(new FileReader(clientKeyPairPath));

        String hostStr = getEnv("CLUSTER_HOSTS");
        hosts = hostStr.split(",");

        factory = new ConnectionFactory();
    }

    protected static String getEnv(String suffix) {
        String result = System.getenv("GOSHAWKDB_" + suffix);
        if (result == null) {
            result = System.getenv("GOSHAWKDB_DEFAULT_" + suffix);
            if (result == null) {
                throw new IllegalArgumentException("No GOSHAWKDB_" + suffix + " env var defined");
            }
        }
        return result;
    }

    protected Connection[] createConnections(int n) throws InterruptedException {
        Connection[] conns = new Connection[n];
        for (int idx = 0; idx < n; idx++) {
            Connection c = factory.connect(certs, hosts[idx % hosts.length]);
            conns[idx] = c;
            connections.add(c);
        }
        return conns;
    }

    protected void inParallel(int parCount, ParRunner runner) throws Exception {
        ConcurrentLinkedDeque<Exception> exceptionQueue = new ConcurrentLinkedDeque<>();
        Connection[] conns = createConnections(parCount);
        Thread[] threads = new Thread[parCount];
        for (int idx = 0; idx < parCount; idx++) {
            int idxCopy = idx;
            threads[idx] = new Thread(() -> {
                Connection conn = conns[idxCopy];
                try {
                    runner.run(idxCopy, conn, exceptionQueue);
                } catch (Exception e) {
                    exceptionQueue.add(e);
                }
            });
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        Exception e = exceptionQueue.peek();
        if (e != null) {
            throw e;
        }
    }

    /**
     * Sets the root object to 8 0-bytes, with no references.
     */
    protected TxnId setRootToZeroInt64(Connection c) throws Exception {
        return c.runTransaction(txn -> {
            GoshawkObj root = txn.getRoot();
            root.set(ByteBuffer.allocate(8));
            return root.getVersion();
        }).result;
    }

    /**
     * Creates n objects, each with 8 0-bytes as their value, and links to all of them from the root
     * object, which has an empty value set.
     */
    protected TxnId setRootToNZeroObjs(Connection c, int n) throws Exception {
        return c.runTransaction(txn -> {
            GoshawkObj[] objs = new GoshawkObj[n];
            for (int idx = 0; idx < n; idx++) {
                objs[idx] = txn.createObject(ByteBuffer.allocate(8));
            }
            GoshawkObj root = txn.getRoot();
            root.set(ByteBuffer.allocate(0), objs);
            return root.getVersion();
        }).result;
    }

    protected TxnId awaitRootVersionChange(Connection c, TxnId oldVsn) throws Exception {
        return c.runTransaction(txn -> {
            if (txn.getRoot().getVersion().equals(oldVsn)) {
                txn.retry();
            }
            return null;
        }).txnid;
    }

    protected void shutdown() throws InterruptedException {
        for (Iterator<Connection> it = connections.iterator(); it.hasNext(); ) {
            Connection conn = it.next();
            if (conn != null) {
                conn.close();
            }
        }
        connections.clear();
        factory.group.shutdownGracefully();
    }

}
