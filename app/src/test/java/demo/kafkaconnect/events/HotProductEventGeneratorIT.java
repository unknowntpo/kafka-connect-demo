package demo.kafkaconnect.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * In-memory integration test for the profile-driven generator.
 *
 * Uses Kafka's MockProducer so we exercise the full runProfile() loop —
 * weighted random, phase selection, inventory state machine, time skew —
 * without needing a real broker. Asserts the invariants that downstream
 * (Connect → Elasticsearch → Kibana) implicitly relies on.
 */
class HotProductEventGeneratorIT {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String PROFILE_JSON =
            "{" +
            "  \"scenario\":\"flash-sale-coupon\"," +
            "  \"profile_version\":\"ai-profile-v1\"," +
            "  \"campaign\":\"test-campaign\"," +
            "  \"total_events\":2000," +
            "  \"duration_seconds\":600," +
            "  \"inventory\":100," +
            "  \"time_skew_power\":2.8," +
            "  \"failure_event\":\"COUPON_CLAIM_FAILED\"," +
            "  \"entity\":{" +
            "    \"product_id\":\"sku_test_001\"," +
            "    \"product_name\":\"Test Coupon\"," +
            "    \"coupon_id\":\"coupon_test_001\"," +
            "    \"coupon_name\":\"Test Coupon Name\"," +
            "    \"discount_percent\":25" +
            "  }," +
            "  \"phases\":[" +
            "    {\"name\":\"teaser\",\"start\":0.0,\"end\":0.2," +
            "     \"event_weights\":{\"COUPON_VIEWED\":60,\"PAGE_REFRESHED\":40}," +
            "     \"failure_weights\":{\"RATE_LIMITED\":100}}," +
            "    {\"name\":\"drop-open\",\"start\":0.2,\"end\":0.7," +
            "     \"event_weights\":{\"COUPON_CLAIM_SUCCEEDED\":50,\"PAGE_REFRESHED\":30,\"COUPON_CLAIM_FAILED\":20}," +
            "     \"failure_weights\":{\"RATE_LIMITED\":80,\"PAYMENT_FAILED\":20}}," +
            "    {\"name\":\"sold-out\",\"start\":0.7,\"end\":1.0," +
            "     \"event_weights\":{\"COUPON_CLAIM_FAILED\":50,\"PAGE_REFRESHED\":50}," +
            "     \"sold_out_event_weights\":{\"COUPON_CLAIM_FAILED\":80,\"PAGE_REFRESHED\":20}," +
            "     \"failure_weights\":{\"COUPON_SOLD_OUT\":100}}" +
            "  ]" +
            "}";

    private HotProductEventGenerator.Config testConfig() {
        HotProductEventGenerator.Config cfg = new HotProductEventGenerator.Config();
        cfg.topic = "test.product.events";
        cfg.seed = 42L;                       // determinism
        cfg.malformedRatio = 0.0;             // disable noise for clean assertions
        cfg.baseTime = "2026-05-01T00:00:00Z";
        return cfg;
    }

    @Test
    void runProfile_producesExpectedRecordCountAndValidJson() throws Exception {
        JsonNode profile = MAPPER.readTree(PROFILE_JSON);
        MockProducer<String, String> producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        HotProductEventGenerator.runProfile(testConfig(), profile, producer);

        List<ProducerRecord<String, String>> history = producer.history();
        assertEquals(2000, history.size(), "should emit exactly total_events records");

        for (ProducerRecord<String, String> rec : history) {
            assertEquals("test.product.events", rec.topic());
            assertNotNull(rec.key());
            JsonNode evt = MAPPER.readTree(rec.value());
            assertTrue(evt.path("event_id").asText().startsWith("evt_"));
            assertNotNull(evt.path("event_type").asText());
            assertNotNull(evt.path("phase").asText());
            assertTrue(evt.has("occurred_at"));
            assertTrue(evt.has("inventory_before"));
            assertTrue(evt.has("inventory_after"));
            assertEquals("flash-sale-coupon", evt.path("scenario").asText());
            assertEquals("test-campaign", evt.path("metadata").path("campaign").asText());
        }
    }

    @Test
    void runProfile_inventoryDecreasesMonotonicallyAndReachesZero() throws Exception {
        JsonNode profile = MAPPER.readTree(PROFILE_JSON);
        MockProducer<String, String> producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        HotProductEventGenerator.runProfile(testConfig(), profile, producer);

        int prevInventory = Integer.MAX_VALUE;
        int minInventory = Integer.MAX_VALUE;
        int successCount = 0;
        for (ProducerRecord<String, String> rec : producer.history()) {
            JsonNode evt = MAPPER.readTree(rec.value());
            int after = evt.path("inventory_after").asInt();
            assertTrue(after >= 0, "inventory must never go negative");
            assertTrue(after <= prevInventory, "inventory must be monotonically non-increasing");
            prevInventory = after;
            minInventory = Math.min(minInventory, after);
            if ("COUPON_CLAIM_SUCCEEDED".equals(evt.path("event_type").asText())) {
                successCount++;
            }
        }
        assertEquals(0, minInventory, "with 2000 events vs inventory 100, stock should reach 0");
        assertEquals(100, successCount, "successful claims must equal initial inventory (100)");
    }

    @Test
    void runProfile_failureEventsCarryFailureReason() throws Exception {
        JsonNode profile = MAPPER.readTree(PROFILE_JSON);
        MockProducer<String, String> producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        HotProductEventGenerator.runProfile(testConfig(), profile, producer);

        Set<String> failureReasonsSeen = new HashSet<>();
        boolean soldOutReasonSeen = false;
        for (ProducerRecord<String, String> rec : producer.history()) {
            JsonNode evt = MAPPER.readTree(rec.value());
            if ("COUPON_CLAIM_FAILED".equals(evt.path("event_type").asText())) {
                String reason = evt.path("failure_reason").asText("");
                assertFalse(reason.isEmpty(), "failure events must include failure_reason");
                failureReasonsSeen.add(reason);
                if ("COUPON_SOLD_OUT".equals(reason)) soldOutReasonSeen = true;
            }
        }
        assertTrue(failureReasonsSeen.size() > 0, "should observe at least one failure reason");
        assertTrue(soldOutReasonSeen, "post-sellout failures should be tagged COUPON_SOLD_OUT");
    }

    @Test
    void runProfile_allFourPhasesAreCovered() throws Exception {
        JsonNode profile = MAPPER.readTree(PROFILE_JSON);
        MockProducer<String, String> producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        HotProductEventGenerator.runProfile(testConfig(), profile, producer);

        Set<String> phasesSeen = new HashSet<>();
        for (ProducerRecord<String, String> rec : producer.history()) {
            JsonNode evt = MAPPER.readTree(rec.value());
            phasesSeen.add(evt.path("phase").asText());
        }
        assertTrue(phasesSeen.contains("teaser"));
        assertTrue(phasesSeen.contains("drop-open"));
        assertTrue(phasesSeen.contains("sold-out"));
    }

    @Test
    void runProfile_isDeterministicForFixedSeed() throws Exception {
        JsonNode profile = MAPPER.readTree(PROFILE_JSON);

        MockProducer<String, String> p1 = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        MockProducer<String, String> p2 = new MockProducer<>(true, new StringSerializer(), new StringSerializer());

        HotProductEventGenerator.runProfile(testConfig(), profile, p1);
        HotProductEventGenerator.runProfile(testConfig(), profile, p2);

        List<ProducerRecord<String, String>> h1 = p1.history();
        List<ProducerRecord<String, String>> h2 = p2.history();
        assertEquals(h1.size(), h2.size());
        for (int i = 0; i < h1.size(); i++) {
            assertEquals(h1.get(i).key(), h2.get(i).key());
            assertEquals(h1.get(i).value(), h2.get(i).value());
        }
    }
}
