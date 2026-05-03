package demo.kafkaconnect.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HotProductEventGeneratorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String PHASES_JSON =
            "[" +
            "  {\"name\":\"teaser\",        \"start\":0.0,  \"end\":0.18}," +
            "  {\"name\":\"waiting-room\",  \"start\":0.18, \"end\":0.42}," +
            "  {\"name\":\"drop-open\",     \"start\":0.42, \"end\":0.7}," +
            "  {\"name\":\"sold-out\",      \"start\":0.7,  \"end\":1.0}" +
            "]";

    @Test
    void findPhaseForProgress_picksByHalfOpenRange() throws Exception {
        JsonNode phases = MAPPER.readTree(PHASES_JSON);

        assertEquals("teaser",       HotProductEventGenerator.findPhaseForProgress(phases, 0.0).path("name").asText());
        assertEquals("teaser",       HotProductEventGenerator.findPhaseForProgress(phases, 0.17).path("name").asText());
        assertEquals("waiting-room", HotProductEventGenerator.findPhaseForProgress(phases, 0.18).path("name").asText());
        assertEquals("drop-open",    HotProductEventGenerator.findPhaseForProgress(phases, 0.42).path("name").asText());
        assertEquals("drop-open",    HotProductEventGenerator.findPhaseForProgress(phases, 0.69999).path("name").asText());
        assertEquals("sold-out",     HotProductEventGenerator.findPhaseForProgress(phases, 0.7).path("name").asText());
    }

    @Test
    void findPhaseForProgress_fallsBackToLastPhaseAtBoundary() throws Exception {
        JsonNode phases = MAPPER.readTree(PHASES_JSON);
        // progress == 1.0 matches no half-open range; should fall back to last phase.
        assertEquals("sold-out", HotProductEventGenerator.findPhaseForProgress(phases, 1.0).path("name").asText());
    }

    @Test
    void findPhaseForProgress_emptyArrayReturnsEmptyNode() throws Exception {
        JsonNode phases = MAPPER.readTree("[]");
        JsonNode result = HotProductEventGenerator.findPhaseForProgress(phases, 0.5);
        assertTrue(result.isObject());
        assertEquals("", result.path("name").asText(""));
    }

    @Test
    void weightedFromNode_distributionMatchesWeights() throws Exception {
        JsonNode weights = MAPPER.readTree("{\"A\":70,\"B\":30}");
        Random random = new Random(1234L);
        Map<String, Integer> counts = new HashMap<>();
        int trials = 10000;
        for (int i = 0; i < trials; i++) {
            counts.merge(HotProductEventGenerator.weightedFromNode(weights, random, "FALLBACK"), 1, Integer::sum);
        }
        // Allow a generous tolerance — purpose is to confirm A dominates and B exists.
        assertTrue(counts.getOrDefault("A", 0) > 6500, "A count should be ~70%, got " + counts.get("A"));
        assertTrue(counts.getOrDefault("B", 0) > 2500, "B count should be ~30%, got " + counts.get("B"));
        assertEquals(0, counts.getOrDefault("FALLBACK", 0));
    }

    @Test
    void weightedFromNode_emptyOrZeroReturnsFallback() throws Exception {
        Random random = new Random(0L);
        assertEquals("DEFAULT", HotProductEventGenerator.weightedFromNode(MAPPER.readTree("{}"), random, "DEFAULT"));
        assertEquals("DEFAULT", HotProductEventGenerator.weightedFromNode(MAPPER.readTree("{\"X\":0}"), random, "DEFAULT"));
    }

    @Test
    void profileEventTimeMillis_skewsLaterInWindow() {
        long base = 0L;
        long duration = 1000L;
        long flatMid = HotProductEventGenerator.profileEventTimeMillis(base, duration, 0.5, 1.0);
        long skewedMid = HotProductEventGenerator.profileEventTimeMillis(base, duration, 0.5, 2.8);

        // Skewed midpoint should land *later* than the flat midpoint — events compress toward the end.
        assertEquals(500L, flatMid);
        assertTrue(skewedMid > flatMid, "skewed mid (" + skewedMid + ") should be > flat mid (" + flatMid + ")");

        // Endpoints are pinned regardless of skew.
        assertEquals(base, HotProductEventGenerator.profileEventTimeMillis(base, duration, 0.0, 2.8));
        assertEquals(base + duration, HotProductEventGenerator.profileEventTimeMillis(base, duration, 1.0, 2.8));
    }

    @Test
    void chooseProfileEventType_switchesToSoldOutWeightsWhenInventoryZero() throws Exception {
        JsonNode phase = MAPPER.readTree(
                "{" +
                "  \"name\":\"sold-out\"," +
                "  \"event_weights\":{\"COUPON_CLAIM_SUCCEEDED\":100}," +
                "  \"sold_out_event_weights\":{\"COUPON_CLAIM_FAILED\":100}" +
                "}");
        Random random = new Random(0L);

        assertEquals("COUPON_CLAIM_SUCCEEDED",
                HotProductEventGenerator.chooseProfileEventType(phase, 50, random));
        assertEquals("COUPON_CLAIM_FAILED",
                HotProductEventGenerator.chooseProfileEventType(phase, 0, random));
    }

    @Test
    void chooseProfileEventType_usesNormalWeightsWhenSoldOutWeightsAbsent() throws Exception {
        JsonNode phase = MAPPER.readTree(
                "{\"name\":\"x\",\"event_weights\":{\"PAGE_REFRESHED\":100}}");
        Random random = new Random(0L);
        // Even with zero inventory, fallback to event_weights when sold_out_event_weights absent.
        assertEquals("PAGE_REFRESHED",
                HotProductEventGenerator.chooseProfileEventType(phase, 0, random));
    }

    @Test
    void weightedFromNode_isDeterministicWithFixedSeed() throws Exception {
        JsonNode weights = MAPPER.readTree("{\"A\":1,\"B\":1,\"C\":1}");
        Random r1 = new Random(42L);
        Random r2 = new Random(42L);
        for (int i = 0; i < 100; i++) {
            assertEquals(
                    HotProductEventGenerator.weightedFromNode(weights, r1, "X"),
                    HotProductEventGenerator.weightedFromNode(weights, r2, "X"));
        }
        // Sanity: different seeds should diverge somewhere in 100 picks.
        Random r3 = new Random(42L);
        Random r4 = new Random(99L);
        boolean diverged = false;
        for (int i = 0; i < 100; i++) {
            String a = HotProductEventGenerator.weightedFromNode(weights, r3, "X");
            String b = HotProductEventGenerator.weightedFromNode(weights, r4, "X");
            if (!a.equals(b)) { diverged = true; break; }
        }
        assertTrue(diverged, "different seeds should produce different sequences");
        assertNotEquals(r3.nextInt(), r4.nextInt());
    }
}
