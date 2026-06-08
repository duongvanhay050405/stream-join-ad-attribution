package com.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamJoinJob {

    // ── Đếm metrics toàn cục ──────────────────────────────────────────
    static final AtomicInteger totalClicks  = new AtomicInteger(0);
    static final AtomicInteger matchedClicks = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        long now = System.currentTimeMillis();

        // ════════════════════════════════════════════════════════════════
        // STREAM A — Ad_Impressions (5 impression bình thường)
        // ════════════════════════════════════════════════════════════════
        DataStream<AdImpressionEvent> impressionStream = env.fromElements(
                new AdImpressionEvent("imp1", "user1", "ad1", now),
                new AdImpressionEvent("imp2", "user2", "ad2", now + 1000L),
                new AdImpressionEvent("imp3", "user3", "ad3", now + 2000L),
                new AdImpressionEvent("imp4", "user4", "ad4", now + 3000L),
                new AdImpressionEvent("imp5", "user5", "ad5", now + 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<AdImpressionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((e, t) -> e.timestamp)
        );

        KeyedStream<AdImpressionEvent, String> impressions =
                impressionStream.keyBy(e -> e.impressionId);

        // ════════════════════════════════════════════════════════════════
        // STREAM B — Ad_Clicks
        //   click1–5  : bình thường, delay 1100–1500ms  → MATCH
        //   click_late : đến sau 15 giây                → MISS (late)
        //   click_orphan: ImpressionID không tồn tại    → MISS (orphan)
        // ════════════════════════════════════════════════════════════════
        DataStream<AdClickEvent> clickStream = env.fromElements(
                // ✅ Normal clicks — trong cửa sổ 10s
                new AdClickEvent("click1",       "imp1", now + 1100L),
                new AdClickEvent("click2",       "imp2", now + 2200L),
                new AdClickEvent("click3",       "imp3", now + 3300L),
                new AdClickEvent("click4",       "imp4", now + 4400L),
                new AdClickEvent("click5",       "imp5", now + 5500L),
                // ❌ FAILURE CASE 1: Late click — đến sau 15s (> 10s window)
                new AdClickEvent("click_late",   "imp1", now + 15000L),
                // ❌ FAILURE CASE 2: Orphan click — imp_fake không có trong buffer
                new AdClickEvent("click_orphan", "imp_fake_999", now + 1000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<AdClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((e, t) -> e.timestamp)
        );

        KeyedStream<AdClickEvent, String> clicks =
                clickStream.keyBy(e -> e.impressionId);

        // ════════════════════════════════════════════════════════════════
        // INTERVAL JOIN — Join window: [0ms, 10s]
        // Click hợp lệ phải đến SAU impression trong vòng 10 giây
        // ════════════════════════════════════════════════════════════════
        DataStream<String> joined = impressions
                .intervalJoin(clicks)
                .between(Time.milliseconds(0), Time.seconds(10))
                .process(new ProcessJoinFunction<AdImpressionEvent, AdClickEvent, String>() {
                    @Override
                    public void processElement(AdImpressionEvent imp,
                                               AdClickEvent click,
                                               Context ctx,
                                               Collector<String> out) {
                        long delay = click.timestamp - imp.timestamp;
                        matchedClicks.incrementAndGet();

                        String result = String.format(
                            "[MATCH] ImpressionID=%-6s | User=%-6s | Ad=%-4s" +
                            " | ClickID=%-12s | delay=%dms",
                            imp.impressionId, imp.userId, imp.adId,
                            click.clickId, delay
                        );
                        out.collect(result);
                    }
                });

        // ════════════════════════════════════════════════════════════════
        // SINK — in ra stdout (thấy trong terminal hoặc Flink Web UI)
        // ════════════════════════════════════════════════════════════════
        joined.print();

        // Ghi chú: click_late và click_orphan sẽ KHÔNG xuất hiện
        // trong joined stream — đó chính là bằng chứng failure case

        System.out.println("================================================");
        System.out.println("LEGEND:");
        System.out.println("  click_late   → MISS: đến sau 15s > cửa sổ 10s");
        System.out.println("  click_orphan → MISS: imp_fake_999 không có trong buffer");
        System.out.println("================================================");

        env.execute("Ad-Click Attribution Join — Stream-to-Stream");

        // ── In Match Rate sau khi job xong ───────────────────────────
        int total   = 7; // 5 normal + 1 late + 1 orphan
        int matched = matchedClicks.get();
        System.out.printf("%n================================================%n");
        System.out.printf("MATCH RATE REPORT%n");
        System.out.printf("  Total clicks   : %d%n", total);
        System.out.printf("  Matched        : %d%n", matched);
        System.out.printf("  Late/Orphan    : %d%n", total - matched);
        System.out.printf("  Match Rate     : %.1f%%%n",
                          matched * 100.0 / total);
        System.out.printf("================================================%n");
    }
}