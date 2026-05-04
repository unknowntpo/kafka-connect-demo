package demo.kafkaconnect.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.nio.charset.StandardCharsets;
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
            if (config.profilePath == null) {
                generate(config, producer);
            } else {
                generateFromProfile(config, producer);
            }
            producer.flush();
        }
    }

    private static void generateFromProfile(Config config, Producer<String, String> producer) throws Exception {
        JsonNode profile = MAPPER.readTree(new File(config.profilePath));
        runProfile(config, profile, producer);
    }

    // Test seam: runs the profile loop against any Producer (real or MockProducer).
    static void runProfile(Config config, JsonNode profile, Producer<String, String> producer) throws Exception {
        Random random = new Random(config.seed == null ? System.nanoTime() : config.seed);
        int totalEvents = intValue(profile, "total_events", Math.max(1, config.ratePerSecond * config.durationSeconds));
        int durationSeconds = intValue(profile, "duration_seconds", config.durationSeconds);
        int inventory = intValue(profile, "inventory", config.initialStock);
        long durationMillis = durationSeconds * 1000L;
        long profileEndMillis = config.baseTime == null ? System.currentTimeMillis() : Instant.parse(config.baseTime).toEpochMilli();
        long baseMillis = profileEndMillis - durationMillis;
        double timeSkewPower = doubleValue(profile, "time_skew_power", 2.0d);
        int participantUsers = Math.max(0, Math.min(totalEvents, intValue(profile, "participant_users", 0)));
        boolean userJourneyMode = participantUsers > 0;
        int remainingInventory = inventory;

        for (int i = 0; i < totalEvents; i++) {
            double progress = (double) i / (double) totalEvents;
            JsonNode phase = findPhaseForProgress(profile.path("phases"), progress);
            boolean entryView = userJourneyMode && i < participantUsers;
            String eventType = entryView ? "COUPON_VIEWED" : chooseProfileEventType(phase, remainingInventory, random);
            if (userJourneyMode && !entryView && "COUPON_VIEWED".equals(eventType)) {
                eventType = "PAGE_REFRESHED";
            }
            String userId = userJourneyMode ? profileParticipantUserId(random, participantUsers, i, entryView) : profileUserId(random, phase);
            int beforeInventory = remainingInventory;
            if (isSuccessEvent(eventType) && remainingInventory <= 0) {
                eventType = failureEventName(profile);
            }
            if (isSuccessEvent(eventType)) {
                remainingInventory = Math.max(0, remainingInventory - 1);
            }

            Map<String, Object> event = buildProfileEvent(profile, phase, eventType, userId, i, beforeInventory, remainingInventory, baseMillis, durationMillis, progress, timeSkewPower, random);
            String eventId = (String) event.get("event_id");
            producer.send(new ProducerRecord<String, String>(config.topic, eventId, MAPPER.writeValueAsString(event)));

            if (config.malformedRatio > 0 && random.nextDouble() < config.malformedRatio) {
                producer.send(new ProducerRecord<String, String>(config.topic, "bad_profile_" + i, "{\"event_id\":\"bad_profile_" + i + "\","));
            }
        }
    }

    private static Map<String, Object> buildProfileEvent(
            JsonNode profile,
            JsonNode phase,
            String eventType,
            String userId,
            int sequence,
            int inventoryBefore,
            int inventoryAfter,
            long baseMillis,
            long durationMillis,
            double progress,
            double timeSkewPower,
            Random random
    ) {
        Map<String, Object> event = new LinkedHashMap<String, Object>();
        String eventIdSource = profile.path("scenario").asText("ai-load") + ":" + eventType + ":" + sequence + ":" + configSafeSeed(profile);
        String eventId = "evt_" + UUID.nameUUIDFromBytes(eventIdSource.getBytes(StandardCharsets.UTF_8)).toString().replace("-", "");
        JsonNode entity = profile.path("entity");
        long occurredAtMillis = profileEventTimeMillis(baseMillis, durationMillis, progress, timeSkewPower);

        event.put("event_id", eventId);
        event.put("event_type", eventType);
        event.put("product_id", entity.path("product_id").asText(PRODUCT_ID));
        event.put("product_name", entity.path("product_name").asText(PRODUCT_NAME));
        event.put("coupon_id", entity.path("coupon_id").asText("coupon_flash_001"));
        event.put("coupon_name", entity.path("coupon_name").asText("Flash Sale Coupon"));
        event.put("scenario", profile.path("scenario").asText("ai-powered-load"));
        event.put("phase", phase.path("name").asText("unknown"));
        event.put("user_id", userId);
        event.put("session_id", "sess_" + Integer.toHexString(random.nextInt()));
        event.put("occurred_at", Instant.ofEpochMilli(occurredAtMillis).toString());
        event.put("service", profileServiceFor(eventType));
        event.put("severity", profileSeverityFor(eventType, inventoryAfter));
        event.put("trace_id", "trace_" + Integer.toHexString(random.nextInt()));
        event.put("remaining_stock", inventoryAfter);
        event.put("remaining_coupons", inventoryAfter);
        event.put("inventory_before", inventoryBefore);
        event.put("inventory_after", inventoryAfter);
        event.put("message", profileMessageFor(eventType, phase.path("name").asText("unknown"), inventoryAfter));

        if (isSuccessEvent(eventType)) {
            event.put("coupon_claim_id", "claim_" + (100000 + sequence));
            event.put("discount_percent", intValue(entity, "discount_percent", 30));
        }
        if (isFailureEvent(eventType)) {
            event.put("failure_reason", inventoryAfter <= 0 ? "COUPON_SOLD_OUT" : weightedFromNode(phase.path("failure_weights"), random, "RATE_LIMITED"));
        }

        Map<String, Object> metadata = new LinkedHashMap<String, Object>();
        metadata.put("region", randomRegion(random));
        metadata.put("campaign", profile.path("campaign").asText("ai-generated-flash-sale"));
        metadata.put("ai_profile_version", profile.path("profile_version").asText("v1"));
        event.put("metadata", metadata);
        return event;
    }

    // Picks the phase whose [start, end) range contains `progress` (0.0–1.0).
    // Ranges are half-open so boundary values (e.g. 0.42) belong to the later phase.
    // Falls back to the last phase when progress == 1.0 or no range matches.
    static JsonNode findPhaseForProgress(JsonNode phases, double progress) {
        if (!phases.isArray() || phases.size() == 0) {
            return MAPPER.createObjectNode();
        }
        JsonNode fallback = phases.get(phases.size() - 1);
        for (JsonNode phase : phases) {
            double start = doubleValue(phase, "start", 0.0d);
            double end = doubleValue(phase, "end", 1.0d);
            if (progress >= start && progress < end) {
                return phase;
            }
        }
        return fallback;
    }

    static String chooseProfileEventType(JsonNode phase, int remainingInventory, Random random) {
        if (remainingInventory <= 0 && phase.has("sold_out_event_weights")) {
            return weightedFromNode(phase.path("sold_out_event_weights"), random, "COUPON_CLAIM_FAILED");
        }
        return weightedFromNode(phase.path("event_weights"), random, "PAGE_REFRESHED");
    }

    static String weightedFromNode(JsonNode weights, Random random, String fallback) {
        if (!weights.isObject() || weights.size() == 0) {
            return fallback;
        }
        int total = 0;
        for (JsonNode value : weights) {
            total += Math.max(0, value.asInt());
        }
        if (total <= 0) {
            return fallback;
        }
        int pick = random.nextInt(total);
        int seen = 0;
        java.util.Iterator<Map.Entry<String, JsonNode>> fields = weights.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            seen += Math.max(0, entry.getValue().asInt());
            if (pick < seen) {
                return entry.getKey();
            }
        }
        return fallback;
    }

    static long profileEventTimeMillis(long baseMillis, long durationMillis, double progress, double timeSkewPower) {
        double shapedProgress = 1.0d - Math.pow(1.0d - progress, Math.max(1.0d, timeSkewPower));
        return baseMillis + Math.round(durationMillis * shapedProgress);
    }

    private static boolean isSuccessEvent(String eventType) {
        return "COUPON_CLAIM_SUCCEEDED".equals(eventType) || "PURCHASE_SUCCEEDED".equals(eventType);
    }

    private static boolean isFailureEvent(String eventType) {
        return "COUPON_CLAIM_FAILED".equals(eventType) || "PURCHASE_FAILED".equals(eventType);
    }

    private static String failureEventName(JsonNode profile) {
        return profile.path("failure_event").asText("COUPON_CLAIM_FAILED");
    }

    private static String profileUserId(Random random, JsonNode phase) {
        int activeUsers = intValue(phase, "active_users", 8000);
        return "user_" + String.format("%05d", random.nextInt(Math.max(1, activeUsers)) + 1);
    }

    private static String profileParticipantUserId(Random random, int participantUsers, int sequence, boolean entryView) {
        int userNumber = entryView ? sequence + 1 : random.nextInt(Math.max(1, participantUsers)) + 1;
        return "user_" + String.format("%05d", userNumber);
    }

    private static String profileServiceFor(String eventType) {
        if ("PAGE_REFRESHED".equals(eventType) || "COUPON_VIEWED".equals(eventType)) {
            return "web";
        }
        if ("WAITING_ROOM_JOINED".equals(eventType)) {
            return "edge-queue";
        }
        return "coupon";
    }

    private static String profileSeverityFor(String eventType, int remainingInventory) {
        if (isFailureEvent(eventType)) {
            return remainingInventory <= 0 ? "WARN" : "ERROR";
        }
        if (remainingInventory <= 100 && isSuccessEvent(eventType)) {
            return "WARN";
        }
        return "INFO";
    }

    private static String profileMessageFor(String eventType, String phase, int remainingInventory) {
        if ("PAGE_REFRESHED".equals(eventType)) {
            return "User repeatedly refreshed during " + phase;
        }
        if ("WAITING_ROOM_JOINED".equals(eventType)) {
            return "User entered waiting room during " + phase;
        }
        if ("COUPON_CLAIM_SUCCEEDED".equals(eventType)) {
            return "Coupon claim succeeded; remaining coupons " + remainingInventory;
        }
        if ("COUPON_CLAIM_FAILED".equals(eventType)) {
            return remainingInventory <= 0 ? "Coupon claim failed because coupons are sold out" : "Coupon claim failed under flash-sale pressure";
        }
        return "Coupon page viewed during " + phase;
    }

    private static int intValue(JsonNode node, String field, int fallback) {
        return node.has(field) ? node.path(field).asInt(fallback) : fallback;
    }

    private static double doubleValue(JsonNode node, String field, double fallback) {
        return node.has(field) ? node.path(field).asDouble(fallback) : fallback;
    }

    private static String configSafeSeed(JsonNode profile) {
        return profile.path("scenario").asText("ai-load") + ":" + profile.path("profile_version").asText("v1");
    }

    private static void generate(Config config, KafkaProducer<String, String> producer) throws JsonProcessingException, InterruptedException {
        Random random = new Random(config.seed == null ? System.nanoTime() : config.seed);
        int totalEvents = Math.max(1, config.ratePerSecond * config.durationSeconds);
        int remainingStock = config.initialStock;
        long nowMillis = config.baseTime == null ? System.currentTimeMillis() : Instant.parse(config.baseTime).toEpochMilli();
        long durationMillis = config.durationSeconds * 1000L;
        long baseMillis = config.sleepBetweenEvents ? nowMillis : nowMillis - durationMillis;

        for (int i = 0; i < totalEvents; i++) {
            double progress = (double) i / (double) totalEvents;
            String eventType = chooseEventType(progress, remainingStock, random);
            if ("PURCHASE_SUCCEEDED".equals(eventType) && remainingStock <= 0) {
                eventType = "PURCHASE_FAILED";
            }

            Map<String, Object> event = buildEvent(eventType, i, remainingStock, eventTimeMillis(baseMillis, durationMillis, progress, config), random);
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

    private static long eventTimeMillis(long baseMillis, long durationMillis, double progress, Config config) {
        if (config.sleepBetweenEvents || config.flatTraffic) {
            return baseMillis + Math.round(durationMillis * progress);
        }

        // Compress more events toward the end of the window to mimic a product going viral.
        double hotSaleProgress = 1.0d - Math.pow(1.0d - progress, 2.2d);
        return baseMillis + Math.round(durationMillis * hotSaleProgress);
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

    static final class Config {
        String topic = DEFAULT_TOPIC;
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP);
        int ratePerSecond = 50;
        int durationSeconds = 8;
        int initialStock = 60;
        Double malformedRatio = 0.01;
        Long seed = 42L;
        boolean sleepBetweenEvents = false;
        boolean flatTraffic = false;
        String profilePath = null;
        String baseTime = null;

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
                } else if (arg.startsWith("--profile=")) {
                    config.profilePath = arg.substring("--profile=".length());
                } else if (arg.startsWith("--base-time=")) {
                    config.baseTime = arg.substring("--base-time=".length());
                } else if (arg.startsWith("--seed=")) {
                    config.seed = Long.parseLong(arg.substring("--seed=".length()));
                } else if ("--no-seed".equals(arg)) {
                    config.seed = null;
                } else if ("--realtime".equals(arg)) {
                    config.sleepBetweenEvents = true;
                } else if ("--flat-traffic".equals(arg)) {
                    config.flatTraffic = true;
                } else {
                    throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }
            return config;
        }
    }
}
