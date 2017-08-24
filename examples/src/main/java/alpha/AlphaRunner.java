package alpha;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class AlphaRunner {
    /** Cache contractor. */
    public static final String CACHE_CONTRACTOR = "CONTRACTORS";

    /** Cache order. */
    public static final String CACHE_ORDER = "ORDERS";

    /** Length of trader length. */
    public static final int TRADER_LEN = 20;

    /** Total number of traders. */
    public static final int TOTAL_TRADERS = 1_000;

    /** Total number of contractors. */
    public static final int TOTAL_CONTRACTORS = 100_000;

    /** Total number of orders. */
    public static final int TOTAL_ORDERS = 1_000_000;

    /**
     * Entry point.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite node = Ignition.start(new IgniteConfiguration().setLocalHost("127.0.0.1"))) {
            // Start caches
            IgniteCache<String, Contractor> cacheContractor = node.getOrCreateCache(cacheContractorConfiguration());
            IgniteCache<OrderPojo.Key, OrderPojo> cacheOrder = node.getOrCreateCache(cacheOrderConfiguration());;

            // Generate traders.
            List<String> traders = new ArrayList<>(TOTAL_TRADERS);

            for (int i = 0; i < TOTAL_TRADERS; i++)
                traders.add(generateTrader(i));

            // Generate contractors.
            List<Contractor> contractors = new ArrayList<>(TOTAL_CONTRACTORS);

            for (int i = 0; i < TOTAL_CONTRACTORS; i++)
                contractors.add(generateContractor(i));

            // Generate orders.
            List<OrderPojo> orders = new ArrayList<>(TOTAL_ORDERS);

            System.out.println(">>> Generated.");

            for (int i = 0; i < TOTAL_ORDERS; i++)
                orders.add(generateOrder(traders, contractors));

            // Inject data.
            try (IgniteDataStreamer<String, Contractor> streamer = node.dataStreamer(CACHE_CONTRACTOR)) {
                for (Contractor contractor : contractors)
                    streamer.addData(contractor.idLe, contractor);

                streamer.flush();
            }

            System.out.println(">>> Loaded contractors.");

            try (IgniteDataStreamer<OrderPojo.Key, OrderPojo> streamer = node.dataStreamer(CACHE_ORDER)) {
                for (OrderPojo order : orders)
                    streamer.addData(new OrderPojo.Key(order.objUID, order.farLeg), order);

                streamer.flush();
            }

            System.out.println(">>> Loaded orders.");

            // Explain.
            String sql = loadSql("sql_01.txt");

//            String explain = (String)cacheOrder.query(new SqlFieldsQuery("EXPLAIN " + sql)).getAll().get(0).get(0);
//
//            System.out.println();
//            System.out.println(">>> EXPLAIN:");
//            System.out.println(explain);
//            System.out.println();

            // Query.
            for (int i = 0; i < 10_000; i++){
                long startTs = System.currentTimeMillis();

                int cnt = 0;

                for (List<?> row : cacheOrder.query(new SqlFieldsQuery(sql)).getAll()) {
                    //System.out.println(row);

                    cnt++;
                }

                long durTs = System.currentTimeMillis() - startTs;

                System.out.println(">>> Count: " + cnt);
                System.out.println(">>> Duration: " + durTs);
                System.out.println();
            }
        }
    }

    public static String generateTrader(int ctr) {
        String ctrLen = String.valueOf(ctr);

        StringBuilder res = new StringBuilder(ctr);

        while (res.length() < TRADER_LEN - ctrLen.length())
            res.append("0");

        res.append(ctrLen);

        return res.toString();
    }

    public static Contractor generateContractor(int ctr) {
        ctr += 100;

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        StringBuilder idLeBuilder = new StringBuilder();

        idLeBuilder.append(ctr);

        for (int i = 0; i < 6; i++)
            idLeBuilder.append((char)rand.nextInt(52, 72));

        idLeBuilder.substring(0, 8);

        Contractor res = new Contractor();

        res.idLe = idLeBuilder.toString();
        res.resident = rand.nextBoolean();

        return res;
    }

    public static OrderPojo generateOrder(List<String> traders, List<Contractor> contractors) {
        ThreadLocalRandom rand = ThreadLocalRandom.current();

        OrderPojo res = new OrderPojo();

        res.objUID = UUID.randomUUID().toString();
        res.farLeg = rand.nextBoolean();

        res.dealType = rand.nextBoolean() ? 1 : 2;

        int buyerIdx = rand.nextInt(contractors.size());
        int sellerIdx = rand.nextInt(contractors.size() - 1);

        if (buyerIdx == sellerIdx)
            sellerIdx++;

        res.buyerLegalEntity = contractors.get(buyerIdx).idLe;
        res.sellerLegalEntity = contractors.get(sellerIdx).idLe;

        res.trader = traders.get(rand.nextInt(traders.size()));

        res.currency1 = rand.nextBoolean() ? "RUR" : "USD";
        res.cur1Amount = new BigDecimal(rand.nextInt(100_000));

        res.currency2 = res.currency1.equals("RUR") ? "USD" : "RUR";
        res.cur2Amount = new BigDecimal(rand.nextInt(100_000));;

        return res;
    }

    /**
     * @return Contractor cache configuration.
     */
    private static CacheConfiguration<String, Contractor> cacheContractorConfiguration() {
        CacheConfiguration<String, Contractor> ccfg = new CacheConfiguration<>(CACHE_CONTRACTOR);

        ccfg.setBackups(1);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setCopyOnRead(true);
        ccfg.setReadFromBackup(true);
        ccfg.setStatisticsEnabled(false);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setRebalanceMode(CacheRebalanceMode.NONE);
//        ccfg.setQueryParallelism(Runtime.getRuntime().availableProcessors() + 4);

        QueryEntity entity = new QueryEntity(String.class, Contractor.class);
        entity.setIndexes(null);

        entity.setIndexes(Collections.singletonList(index("ID_LE", 20)));

        ccfg.setQueryEntities(Collections.singletonList(entity));

        return ccfg;
    }

    /**
     * @return Contractor cache configuration.
     */
    private static CacheConfiguration<OrderPojo.Key, OrderPojo> cacheOrderConfiguration() {
        CacheConfiguration<OrderPojo.Key, OrderPojo> ccfg = new CacheConfiguration<>(CACHE_ORDER);

        ccfg.setBackups(1);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setCopyOnRead(true);
        ccfg.setReadFromBackup(true);
        ccfg.setStatisticsEnabled(false);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setQueryParallelism(4);
        ccfg.setRebalanceMode(CacheRebalanceMode.NONE);

        ccfg.setIndexedTypes(OrderPojo.Key.class, OrderPojo.class);

        QueryEntity entity = new QueryEntity(OrderPojo.Key.class, OrderPojo.class);
        entity.setIndexes(null);

        Collection<QueryIndex> idxs = new ArrayList<>();

        idxs.add(index("BUYER_LEGAL_ENTITY", 20));
        idxs.add(index("SELLER_LEGAL_ENTITY", 20));

        idxs.add(index("TRADER"));
        idxs.add(index("CURRENCY1"));
        idxs.add(index("CURRENCY2"));

//        idxs.add(index("TRADE_DATE"));
//        idxs.add(index("VALUE_DATE"));
//        idxs.add(index("UNDERLYING_TYPE"));

        entity.setIndexes(idxs);

        ccfg.setQueryEntities(Collections.singletonList(entity));

        return ccfg;
    }

    /**
     * @param name Field name.
     * @return Index.
     */
    private static QueryIndex index(String name) {
        return new QueryIndex(name);
    }

    /**
     * @param name Field name.
     * @param inlineSize Inline size.
     * @return Index.
     */
    private static QueryIndex index(String name, int inlineSize) {
        QueryIndex idx = new QueryIndex(name);

        idx.setInlineSize(inlineSize);

        return idx;
    }

    /**
     * Load SQL from a file.
     *
     * @param file File.
     * @return SQL.
     * @throws Exception If failed.
     */
    private static String loadSql(String file) throws Exception {
        StringBuilder res = new StringBuilder();

        String path = "C:\\Personal\\code\\incubator-ignite\\examples\\src\\main\\java\\alpha\\" + file;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)))) {
            String next = br.readLine();

            while (next != null) {
                res.append(next).append("\n");

                next = br.readLine();
            }
        }

        return res.toString();
    }
}
