/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class MvccTestApp {
    /** */
    private static final boolean DEBUG_LOG = false;

    public static void main0(String[] args) throws Exception {
        final TestCluster cluster = new TestCluster(3);

        final int ACCOUNTS = 3;

        final int START_VAL = 100_000;

        final Map<Object, Object> data = new TreeMap<>();

        for (int i = 0; i < ACCOUNTS; i++)
            data.put(i, START_VAL);

        cluster.txPutAll(data);

        cluster.txTransfer(0, 1, true);
        cluster.txTransfer(0, 1, true);
        cluster.txTransfer(0, 2, true);

        Map<Object, Object> getData = cluster.getAll(data.keySet());

        int sum = 0;

        for (int i = 0; i < ACCOUNTS; i++) {
            Integer val = (Integer)getData.get(i);

            sum += val;

            System.out.println("Val: " + val);
        }

        System.out.println("Sum: " + sum);
    }

    public static void main(String[] args) throws Exception {
        final AtomicBoolean err = new AtomicBoolean();

        for (int iter = 0; iter < 3; iter++) {
            System.out.println("Iteration: " + iter);

            final TestCluster cluster = new TestCluster(1);

            final int ACCOUNTS = 5;

            final int START_VAL = 100;

            final Map<Object, Object> data = new TreeMap<>();

            for (int i = 0; i < ACCOUNTS; i++)
                data.put(i, START_VAL);

            cluster.txPutAll(data);

            final AtomicBoolean stop = new AtomicBoolean();

            List<Thread> threads = new ArrayList<>();

            for (int i = 0; i < 1; i++) {
                final int id = i;

                Thread thread = new Thread(new Runnable() {
                    @Override public void run() {
                        Thread.currentThread().setName("read" + id);

                        int cnt = 0;

                        while (!stop.get()) {
                            Map<Object, Object> getData = cluster.getAll(data.keySet());

                            cnt++;

                            int sum = 0;

                            for (int i = 0; i < ACCOUNTS; i++) {
                                Integer val = (Integer)getData.get(i);

                                sum += val;
                            }

                            if (sum != ACCOUNTS * START_VAL) {
                                if (stop.compareAndSet(false, true)) {
                                    stop.set(true);
                                    err.set(true);

                                    System.out.println("Invalid get sum: " + sum);
                                }
                            }
                        }

                        System.out.println("Get cnt: " + cnt);
                    }
                });

                threads.add(thread);

                thread.start();
            }

            for (int i = 0; i < 2; i++) {
                final int id = i;

                Thread thread = new Thread(new Runnable() {
                    @Override public void run() {
                        Thread.currentThread().setName("update" + id);

                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        while (!stop.get()) {
                            int id1 = rnd.nextInt(ACCOUNTS);

                            int id2 = rnd.nextInt(ACCOUNTS);

                            while (id2 == id1)
                                id2 = rnd.nextInt(ACCOUNTS);

                            if (id1 > id2) {
                                int tmp = id1;
                                id1 = id2;
                                id2 = tmp;
                            }

                            cluster.txTransfer(id1, id2, rnd.nextBoolean());
                        }

                    }
                });

                threads.add(thread);

                thread.start();
            }

            long endTime = System.currentTimeMillis() + 5000;

            while (!stop.get()) {
                Thread.sleep(100);

                if (System.currentTimeMillis() >= endTime)
                    break;
            }

            stop.set(true);

            for (Thread thread : threads)
                thread.join();

            Map<Object, Object> getData = cluster.getAll(data.keySet());

            int sum = 0;

            for (int i = 0; i < ACCOUNTS; i++) {
                Integer val = (Integer)getData.get(i);

                System.out.println("Val " + val);

                sum += val;
            }

            System.out.println("Sum=" + sum + ", expSum=" + (ACCOUNTS * START_VAL));

            if (err.get()) {
                System.out.println("Error!");

                System.exit(1);
            }
        }
    }

    /**
     *
     */
    static class TestCluster {
        /** */
        final List<Node> nodes = new ArrayList<>();

        /** */
        final Coordinator crd;

        /** */
        final AtomicLong txIdGen = new AtomicLong(10_000);

        TestCluster(int nodesNum) {
            crd = new Coordinator();

            for (int i = 0; i < nodesNum; i++)
                nodes.add(new Node(i));
        }

        void txPutAll(Map<Object, Object> data) {
            TxId txId = new TxId(txIdGen.incrementAndGet());

            Map<Object, Node> mappedEntries = new LinkedHashMap<>();

            for (Object key : data.keySet()) {
                int nodeIdx = nodeForKey(key);

                Node node = nodes.get(nodeIdx);

                node.dataStore.lockEntry(key);

                mappedEntries.put(key, node);
            }

            CoordinatorCounter cntr = crd.nextTxCounter(txId);

            MvccUpdateVersion mvccVer = new MvccUpdateVersion(cntr, txId);

            for (Map.Entry<Object, Node> e : mappedEntries.entrySet()) {
                Node node = e.getValue();

                node.dataStore.updateEntry(e.getKey(), data.get(e.getKey()), mvccVer);

                node.dataStore.unlockEntry(e.getKey());
            }

            crd.txDone(txId);
        }

        void txTransfer(Integer id1, Integer id2, boolean fromFirst) {
            TreeSet<Integer> keys = new TreeSet<>();

            keys.add(id1);
            keys.add(id2);

            TxId txId = new TxId(txIdGen.incrementAndGet());

            Map<Object, Node> mappedEntries = new LinkedHashMap<>();

            Map<Object, Object> vals = new HashMap<>();

            for (Object key : keys) {
                int nodeIdx = nodeForKey(key);

                Node node = nodes.get(nodeIdx);

                node.dataStore.lockEntry(key);

                vals.put(key, node.dataStore.lastValue(key));

                mappedEntries.put(key, node);
            }

            CoordinatorCounter cntr = crd.nextTxCounter(txId);

            Integer curVal1 = (Integer)vals.get(id1);
            Integer curVal2 = (Integer)vals.get(id2);

            boolean update = false;

            Map<Object, Object> newVals = new HashMap<>();

            Integer newVal1 = null;
            Integer newVal2 = null;

            if (fromFirst) {
                if (curVal1 > 0) {
                    update = true;

                    newVal1 = curVal1 - 1;
                    newVal2 = curVal2 + 1;
                }
            }
            else {
                if (curVal2 > 0) {
                    update = true;

                    newVal1 = curVal1 + 1;
                    newVal2 = curVal2 - 1;
                }
            }

            if (update) {
                newVals.put(id1, newVal1);
                newVals.put(id2, newVal2);

                MvccUpdateVersion mvccVer = new MvccUpdateVersion(cntr, txId);

                if (DEBUG_LOG) {
                    log("update txId=" + txId +
                        ", id1=" + id1 + ", v1=" + newVal1 +
                        ", id2=" + id2 + ", v2=" + newVal2 +
                        ", ver=" + cntr);
                }

                for (Map.Entry<Object, Node> e : mappedEntries.entrySet()) {
                    Node node = e.getValue();

                    node.dataStore.updateEntry(e.getKey(), newVals.get(e.getKey()), mvccVer);
                }

                for (Map.Entry<Object, Node> e : mappedEntries.entrySet()) {
                    Node node = e.getValue();

                    node.dataStore.unlockEntry(e.getKey());
                }
            }
            else {
                for (Map.Entry<Object, Node> e : mappedEntries.entrySet()) {
                    Node node = e.getValue();

                    node.dataStore.unlockEntry(e.getKey());
                }
            }

            if (DEBUG_LOG)
                log("tx done " + txId);

            crd.txDone(txId);
        }

        public Map<Object, Object> getAll(Set<?> keys) {
            MvccQueryVersion ver = crd.queryVersion();

            Map<Object, Object> res = new HashMap<>();

            for (Object key : keys) {
                int nodeIdx = nodeForKey(key);

                Node node = nodes.get(nodeIdx);

                Object val = node.dataStore.get(key, ver);

                res.put(key, val);
            }

            crd.queryDone(ver.cntr);

            if (DEBUG_LOG)
                log("query [cntr=" + ver.cntr + ", txs=" + ver.activeTxs + ", res=" + res + ']');

            return res;
        }

        private int nodeForKey(Object key) {
            return U.safeAbs(key.hashCode()) % nodes.size();
        }
    }

    /**
     *
     */
    static class Node {
        /** */
        final DataStore dataStore;

        /** */
        final int nodexIdx;

        public Node(int nodexIdx) {
            this.nodexIdx = nodexIdx;

            dataStore = new DataStore();
        }

        @Override public String toString() {
            return "Node [idx=" + nodexIdx + ']';
        }
    }

    /**
     *
     */
    static class Coordinator {
        /** */
        private final AtomicLong cntr = new AtomicLong(-1);

        /** */
        private final GridAtomicLong commitCntr = new GridAtomicLong(-1);

        /** */
        @GridToStringInclude
        private final ConcurrentHashMap<TxId, Long> activeTxs = new ConcurrentHashMap<>();

        CoordinatorCounter nextTxCounter(TxId txId) {
            final CoordinatorCounter newCtr = new CoordinatorCounter(cntr.incrementAndGet());

            Long old = activeTxs.put(txId, newCtr.cntr);

            assert old == null;

            return newCtr;
        }

        void txDone(TxId txId) {
            Long cntr = activeTxs.remove(txId);

            assert cntr != null;

            commitCntr.setIfGreater(cntr);
        }

        MvccQueryVersion queryVersion() {
            Set<TxId> txs = new HashSet<>();

            Long minActive = null;

            for (Map.Entry<TxId, Long> e : activeTxs.entrySet()) {
                txs.add(e.getKey());

                if (minActive == null)
                    minActive = e.getValue();
                else if (e.getValue() < minActive)
                    minActive = e.getValue();
            }

            long cntr = commitCntr.get();

            if (minActive != null && minActive < cntr)
                cntr = minActive;

            return new MvccQueryVersion(new CoordinatorCounter(cntr), txs);
        }

        void queryDone(CoordinatorCounter ctr) {

        }

        @Override public String toString() {
            return S.toString(Coordinator.class, this);
        }
    }

    /**
     *
     */
    static class CoordinatorCounter implements Comparable<CoordinatorCounter> {
        /** */
        private final long topVer; // TODO

        /** */
        private final long cntr;

        CoordinatorCounter(long cntr) {
            this.topVer = 1;
            this.cntr = cntr;
        }

        @Override public int compareTo(CoordinatorCounter o) {
            return Long.compare(cntr, o.cntr);
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            CoordinatorCounter that = (CoordinatorCounter)o;

            return cntr == that.cntr;
        }

        @Override public int hashCode() {
            return (int)(cntr ^ (cntr >>> 32));
        }

        @Override public String toString() {
            return "Cntr [c=" + cntr + ']';
        }
    }

    /**
     *
     */
    static class MvccUpdateVersion {
        /** */
        @GridToStringInclude
        final CoordinatorCounter cntr;

        /** */
        @GridToStringInclude
        final TxId txId;

        /**
         * @param cntr
         */
        MvccUpdateVersion(CoordinatorCounter cntr, TxId txId) {
            assert cntr != null;

            this.cntr = cntr;
            this.txId = txId;
        }

        @Override public String toString() {
            return S.toString(MvccUpdateVersion.class, this);
        }
    }

    /**
     *
     */
    static class MvccQueryVersion {
        /** */
        @GridToStringInclude
        final CoordinatorCounter cntr;

        /** */
        @GridToStringInclude
        final Collection<TxId> activeTxs;

        MvccQueryVersion(CoordinatorCounter cntr, Collection<TxId> activeTxs) {
            this.cntr = cntr;
            this.activeTxs = activeTxs;
        }

        @Override public String toString() {
            return S.toString(MvccQueryVersion.class, this);
        }
    }

    /**
     *
     */
    static class TxId {
        /** */
        @GridToStringInclude
        final long id;

        TxId(long id) {
            this.id = id;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TxId txId = (TxId) o;

            return id == txId.id;
        }

        @Override public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }

        @Override public String toString() {
            return S.toString(TxId.class, this);
        }
    }

    /**
     *
     */
    static class DataStore {
        /** */
        private final ConcurrentHashMap<Object, ReentrantLock> locks = new ConcurrentHashMap<>();

        /** */
        private final ConcurrentHashMap<Object, MvccValue> mainIdx = new ConcurrentHashMap<>();

        /** */
        private final ConcurrentHashMap<Object, List<MvccValue>> mvccIdx = new ConcurrentHashMap<>();

        void lockEntry(Object key) {
            ReentrantLock e = lock(key);

            e.lock();
        }

        void unlockEntry(Object key) {
            ReentrantLock e = lock(key);

            e.unlock();
        }

        void updateEntry(Object key, Object val, MvccUpdateVersion ver) {
            List<MvccValue> list = mvccIdx.get(key);

            if (list == null)
                mvccIdx.put(key, list = new ArrayList<>());

            synchronized (list) {
                list.add(new MvccValue(val, ver));
            }
        }

        Object lastValue(Object key) {
            List<MvccValue> list = mvccIdx.get(key);

            if (list != null)
                return list.get(list.size() - 1).val;

            MvccValue val = mainIdx.get(key);

            return val != null ? val.val : null;
        }

        Object get(Object key, MvccQueryVersion ver) {
            List<MvccValue> list = mvccIdx.get(key);

            if (list != null) {
                synchronized (list) {
                    for (int i = list.size() - 1; i >= 0; i--) {
                        MvccValue val = list.get(i);

                        int cmp = val.ver.cntr.compareTo(ver.cntr);

                        if (cmp > 0)
                            continue;

                        if (ver.activeTxs.contains(val.ver.txId))
                            continue;

                        if (DEBUG_LOG)
                            log("get res [key=" + key + ", val=" + val.val + ", ver=" + val.ver + ']');

                        return val.val;
                    }
                }
            }

            MvccValue val = mainIdx.get(key);

            return val != null ? val.val : null;
        }

        private ReentrantLock lock(Object key) {
            ReentrantLock e = locks.get(key);

            if (e == null) {
                ReentrantLock old = locks.putIfAbsent(key, e = new ReentrantLock());

                if (old != null)
                    e = old;
            }

            return e;
        }
    }

    /**
     *
     */
    static class MvccValue {
        /** */
        @GridToStringInclude
        final Object val;

        /** */
        @GridToStringInclude
        final MvccUpdateVersion ver;

        MvccValue(Object val, MvccUpdateVersion ver) {
            assert ver != null;

            this.val = val;
            this.ver = ver;
        }

        @Override public String toString() {
            return S.toString(MvccValue.class, this);
        }
    }

    static void log(String msg) {
        System.out.println(Thread.currentThread() + ": " + msg);
    }
}

class TestDebugLog {
    /** */
    private static final List<Object> msgs = Collections.synchronizedList(new ArrayList<>(100_000));

    /** */
    private static final SimpleDateFormat DEBUG_DATE_FMT = new SimpleDateFormat("HH:mm:ss,SSS");

    static class Message {
        String thread = Thread.currentThread().getName();

        String msg;

        long ts = U.currentTimeMillis();

        public Message(String msg) {
            this.msg = msg;
        }

        public String toString() {
            return "Msg [msg=" + msg + ", thread=" + thread + ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';
        }
    }

    static class EntryMessage extends Message {
        Object key;
        Object val;

        public EntryMessage(Object key, Object val, String msg) {
            super(msg);

            this.key = key;
            this.val = val;
        }

        public String toString() {
            return "EntryMsg [key=" + key + ", val=" + val + ", msg=" + msg + ", thread=" + thread + ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';
        }
    }

    static class PartMessage extends Message {
        int p;
        Object val;

        public PartMessage(int p, Object val, String msg) {
            super(msg);

            this.p = p;
            this.val = val;
        }

        public String toString() {
            return "PartMessage [p=" + p + ", val=" + val + ", msg=" + msg + ", thread=" + thread + ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';
        }
    }

    static final boolean out = false;

    public static void addMessage(String msg) {
        msgs.add(new Message(msg));

        if (out)
            System.out.println(msg);
    }

    public static void addEntryMessage(Object key, Object val, String msg) {
        if (key instanceof KeyCacheObject)
            key = ((KeyCacheObject)key).value(null, false);

        EntryMessage msg0 = new EntryMessage(key, val, msg);

        msgs.add(msg0);

        if (out) {
            System.out.println(msg0.toString());

            System.out.flush();
        }
    }

    public static void addPartMessage(int p, Object val, String msg) {
        PartMessage msg0 = new PartMessage(p, val, msg);

        msgs.add(msg0);

        if (out) {
            System.out.println(msg0.toString());

            System.out.flush();
        }
    }

    public static void printMessages(boolean file, Integer part) {
        List<Object> msgs0;

        synchronized (msgs) {
            msgs0 = new ArrayList<>(msgs);

            msgs.clear();
        }

        if (file) {
            try {
                FileOutputStream out = new FileOutputStream("test_debug.log");

                PrintWriter w = new PrintWriter(out);

                for (Object msg : msgs0) {
                    if (part != null && msg instanceof PartMessage) {
                        if (((PartMessage) msg).p != part)
                            continue;
                    }

                    w.println(msg.toString());
                }

                w.close();

                out.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            for (Object msg : msgs0)
                System.out.println(msg);
        }
    }

    public static void printKeyMessages(boolean file, Object key) {
        List<Object> msgs0;

        synchronized (msgs) {
            msgs0 = new ArrayList<>(msgs);

            msgs.clear();
        }

        if (file) {
            try {
                FileOutputStream out = new FileOutputStream("test_debug.log");

                PrintWriter w = new PrintWriter(out);

                for (Object msg : msgs0) {
                    if (msg instanceof EntryMessage && !((EntryMessage)msg).key.equals(key))
                        continue;

                    w.println(msg.toString());
                }

                w.close();

                out.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            for (Object msg : msgs0) {
                if (msg instanceof EntryMessage && !((EntryMessage)msg).key.equals(key))
                    continue;

                System.out.println(msg);
            }
        }
    }

    public static void clear() {
        msgs.clear();
    }

    public static void clearEntries() {
        for (Iterator it = msgs.iterator(); it.hasNext();) {
            Object msg = it.next();

            if (msg instanceof EntryMessage)
                it.remove();
        }
    }
}