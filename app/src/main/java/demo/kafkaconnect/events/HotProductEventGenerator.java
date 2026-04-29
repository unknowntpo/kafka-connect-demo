package demo.kafkaconnect.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public final class HotProductEventGenerator {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DEFAULT_TOPIC = "product.events";
    private static final String DEFAULT_BOOTSTRAP = "host.docker.internal:9092";
    private static final String PRODUCT_ID = "sku_hot_001";
    private static final String PRODUCT_NAME = "Limited Edition Keyboard";
    private static final BigDecimal PRICE = new BigDecimal("129.99");

    private HotProductEventGenerator() {
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.parse(args);
        Properties props = new Properties();
        props.put("bootstrap.servers", config.bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props)) {
            generate(config, producer);
            producer.flush();
        }
    }

    private static void generate(Config config, KafkaProducer<String, String> producer) throws JsonProcessingException, InterruptedException {
        Random random = new Random(config.seed == null ? System.nanoTime() : config.seed);
        int totalEvents = Math.max(1, config.ratePerSecond * config.durationSeconds);
        int remainingStock = config.initialStock;
        long baseMillis = System.currentTimeMillis();
        long spacingMillis = Math.max(1L, (config.durationSeconds * 1000L) / totalEvents);

        for (int i = 0; i < totalEvents; i++) {
            double progress = (double) i / (double) totalEvents;
            String eventType = chooseEventType(progress, remainingStock, random);
            if ("PURCHASE_SUCCEEDED".equals(eventType) && remainingStock <= 0) {
                eventType = "PURCHASE_FAILED";
            }

            Map<String, Object> event = buildEvent(eventType, i, remainingStock, baseMillis + (i * spacingMillis), random);
            if ("PURCHASE_SUCCEEDED".equals(eventType)) {
                remainingStock = Math.max(0, remainingStock - 1);
                event.put("remaining_stock", remainingStock);
            }
            if ("STOCK_CHANGED".equals(eventType)) {
                event.put("remaining_stock", remainingStock);
            }
            if ("PURCHASE_FAILED".equals(eventType) && remainingStock <= 0) {
                event.put("failure_reason", "OUT_OF_STOCK");
                event.put("remaining_stock", 0);
            }

            String eventId = (String) event.get("event_id");
            producer.send(new ProducerRecord<String, String>(config.topic, eventId, MAPPER.writeValueAsString(event)));

            if (config.malformedRatio > 0 && random.nextDouble() < config.malformedRatio) {
                producer.send(new ProducerRecord<String, String>(config.topic, "bad_" + i, "{\"event_id\":\"bad_" + i + "\","));
            }

            if (config.sleepBetweenEvents) {
                Thread.sleep(Math.max(1L, 1000L / Math.max(1, config.ratePerSecond)));
            }
        }
    }

    private static String chooseEventType(double progress, int remainingStock, Random random) {
        if (remainingStock <= 0) {
            return random.nextDouble() < 0.65 ? "PURCHASE_FAILED" : randomViewOrClick(random);
        }
        if (progress < 0.25) {
            return weighted(random, "PRODUCT_VIEWED", 70, "BUY_CLICKED", 25, "PURCHASE_SUCCEEDED", 5);
        }
        if (progress < 0.75) {
            return weighted(random, "PRODUCT_VIEWED", 35, "BUY_CLICKED", 30, "PURCHASE_SUCCEEDED", 30, "STOCK_CHANGED", 5);
        }
        return weighted(random, "BUY_CLICKED", 35, "PURCHASE_SUCCEEDED", 35, "PURCHASE_FAILED", 20, "PRODUCT_VIEWED", 10);
    }

    private static String randomViewOrClick(Random random) {
        return random.nextBoolean() ? "PRODUCT_VIEWED" : "BUY_CLICKED";
    }

    private static String weighted(Random random, Object... pairs) {
        int total = 0;
        for (int i = 1; i < pairs.length; i += 2) {
            total += (Integer) pairs[i];
        }
        int pick = random.nextInt(total);
        int seen = 0;
        for (int i = 0; i < pairs.length; i += 2) {
            seen += (Integer) pairs[i + 1];
            if (pick < seen) {
                return (String) pairs[i];
            }
        }
        return (String) pairs[0];
    }

    private static Map<String, Object> buildEvent(String eventType, int sequence, int remainingStock, long occurredAtMillis, Random random) {
        Map<String, Object> event = new LinkedHashMap<String, Object>();
        String eventId = "evt_" + UUID.nameUUIDFromBytes((eventType + ":" + sequence + ":" + occurredAtMillis).getBytes()).toString().replace("-", "");
        event.put("event_id", eventId);
        event.put("event_type", eventType);
        event.put("product_id", PRODUCT_ID);
        event.put("product_name", PRODUCT_NAME);
        event.put("user_id", "user_" + String.format("%04d", random.nextInt(800) + 1));
        event.put("session_id", "sess_" + Integer.toHexString(random.nextInt()));
        event.put("occurred_at", Instant.ofEpochMilli(occurredAtMillis).toString());
        event.put("service", serviceFor(eventType));
        event.put("severity", severityFor(eventType, remainingStock));
        event.put("trace_id", "trace_" + Integer.toHexString(random.nextInt()));
        event.put("remaining_stock", remainingStock);
        event.put("message", messageFor(eventType, remainingStock));

        if ("PURCHASE_SUCCEEDED".equals(eventType)) {
            event.put("order_id", "order_" + (9000 + sequence));
            event.put("price", PRICE.setScale(2, RoundingMode.HALF_UP));
        }
        if ("PURCHASE_FAILED".equals(eventType)) {
            event.put("failure_reason", remainingStock <= 0 ? "OUT_OF_STOCK" : randomFailure(random));
        }

        Map<String, Object> metadata = new LinkedHashMap<String, Object>();
        metadata.put("region", randomRegion(random));
        metadata.put("campaign", progressCampaign(sequence));
        event.put("metadata", metadata);
        return event;
    }

    private static String serviceFor(String eventType) {
        if ("PRODUCT_VIEWED".equals(eventType) || "BUY_CLICKED".equals(eventType)) {
            return "web";
        }
        if ("PURCHASE_SUCCEEDED".equals(eventType) || "PURCHASE_FAILED".equals(eventType)) {
            return "checkout";
        }
        return "inventory";
    }

    private static String severityFor(String eventType, int remainingStock) {
        if ("PURCHASE_FAILED".equals(eventType)) {
            return remainingStock <= 0 ? "WARN" : "ERROR";
        }
        if (remainingStock <= 10 && ("STOCK_CHANGED".equals(eventType) || "PURCHASE_SUCCEEDED".equals(eventType))) {
            return "WARN";
        }
        return "INFO";
    }

    private static String messageFor(String eventType, int remainingStock) {
        if ("PRODUCT_VIEWED".equals(eventType)) {
            return "Hot product page viewed";
        }
        if ("BUY_CLICKED".equals(eventType)) {
            return "User clicked buy button";
        }
        if ("PURCHASE_SUCCEEDED".equals(eventType)) {
            return "Purchase succeeded; stock is now " + Math.max(0, remainingStock - 1);
        }
        if ("PURCHASE_FAILED".equals(eventType)) {
            return remainingStock <= 0 ? "Purchase failed because product is sold out" : "Purchase failed during checkout";
        }
        return "Stock level changed";
    }

    private static String randomFailure(Random random) {
        String[] reasons = {"PAYMENT_FAILED", "RATE_LIMITED"};
        return reasons[random.nextInt(reasons.length)];
    }

    private static String randomRegion(Random random) {
        String[] regions = {"ap-northeast-1", "us-east-1", "eu-west-1"};
        return regions[random.nextInt(regions.length)];
    }

    private static String progressCampaign(int sequence) {
        return sequence % 3 == 0 ? "creator-drop" : "organic";
    }

    private static final class Config {
        private String topic = DEFAULT_TOPIC;
        private String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP);
        private int ratePerSecond = 50;
        private int durationSeconds = 8;
        private int initialStock = 60;
        private Double malformedRatio = 0.01;
        private Long seed = 42L;
        private boolean sleepBetweenEvents = false;

        private static Config parse(String[] args) {
            Config config = new Config();
            for (String arg : args) {
                if ("generate".equals(arg)) {
                    continue;
                }
                if (arg.startsWith("--topic=")) {
                    config.topic = arg.substring("--topic=".length());
                } else if (arg.startsWith("--bootstrap-servers=")) {
                    config.bootstrapServers = arg.substring("--bootstrap-servers=".length());
                } else if (arg.startsWith("--rate-per-second=")) {
                    config.ratePerSecond = Integer.parseInt(arg.substring("--rate-per-second=".length()));
                } else if (arg.startsWith("--duration-seconds=")) {
                    config.durationSeconds = Integer.parseInt(arg.substring("--duration-seconds=".length()));
                } else if (arg.startsWith("--initial-stock=")) {
                    config.initialStock = Integer.parseInt(arg.substring("--initial-stock=".length()));
                } else if (arg.startsWith("--malformed-ratio=")) {
                    config.malformedRatio = Double.parseDouble(arg.substring("--malformed-ratio=".length()));
                } else if (arg.startsWith("--seed=")) {
                    config.seed = Long.parseLong(arg.substring("--seed=".length()));
                } else if ("--no-seed".equals(arg)) {
                    config.seed = null;
                } else if ("--realtime".equals(arg)) {
                    config.sleepBetweenEvents = true;
                } else {
                    throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }
            return config;
        }
    }
}
