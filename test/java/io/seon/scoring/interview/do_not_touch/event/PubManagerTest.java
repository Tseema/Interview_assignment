package io.seon.scoring.interview.do_not_touch.event;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import io.seon.scoring.interview.do_not_touch.dto.Table;
import io.seon.scoring.interview.do_not_touch.dto.TableState;
import io.seon.scoring.interview.work_here.PubManager;


@TestMethodOrder(OrderAnnotation.class)
@SpringBootTest
class PubManagerTest {

    private static final Table TABLE_1 = Table.of(1);
    private static final Table TABLE_2 = Table.of(2);
    private static final Table TABLE_3 = Table.of(3);


    @Autowired
    private PubService pubService;

    @Autowired
    private PubManager pubManager;

    @BeforeEach
    public void init() {
        pubService.emittedEvents.clear();
    }

    @Test
    @Order(1)
    @DisplayName("Pub is opened for service in 1 sec")
    void test_pub_opened_for_service() {
        initAndAssertInit(1, 2);

        assertEquals(2, pubService.tables.size());
    }

    @Test
    @Order(2)
    @DisplayName("1 Bartender - 1 Table - Simple workflow")
    void test_1_bartender_1_table_simple() {
        initAndAssertInit(1, 1);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_ARRIVED));
        assertTableState(TABLE_1.getId(), TableState.TAKEN);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_DRINKS_DONE));
        assertTableState(TABLE_1.getId(), TableState.DRINKING);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_LEFT));
        assertTableState(TABLE_1.getId(), TableState.EMPTY);
    }


    @Test
    @Order(3)
    @DisplayName("1 Bartender - 1 Table - Table with drink duration")
    void test_1_bartender_1_table_with_drink_duration() {
        initAndAssertInit(1, 1);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_ARRIVED));
        assertTableState(TABLE_1.getId(), TableState.TAKEN);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_DRINKS_DONE, "500"));
        assertTableState(TABLE_1.getId(), TableState.TAKEN);
        assertTableState(TABLE_1.getId(), TableState.DRINKING, 500, 1_000);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_LEFT));
        assertTableState(TABLE_1.getId(), TableState.EMPTY);
    }

    @Test
    @Order(4)
    @DisplayName("1 Bartender - 2 Tables - Simple workflow")
    void test_1_bartender_2_tables() {
        initAndAssertInit(1, 2);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_ARRIVED));
        assertTableState(TABLE_1.getId(), TableState.TAKEN);
        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_ARRIVED));
        assertTableState(TABLE_2.getId(), TableState.TAKEN);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_DRINKS_DONE));
        assertTableState(TABLE_1.getId(), TableState.DRINKING);
        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_DRINKS_DONE));
        assertTableState(TABLE_2.getId(), TableState.DRINKING);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_LEFT));
        assertTableState(TABLE_1.getId(), TableState.EMPTY);
        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_LEFT));
        assertTableState(TABLE_2.getId(), TableState.EMPTY);
    }

    @Test
    @Order(5)
    @DisplayName("2 Bartenders - 2 Tables - Drink duration on table 1 mustn't block table 2")
    void test_2_bartenders_2_tables_drink_duration_table1() {
        initAndAssertInit(2, 2);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_ARRIVED));
        assertTableState(TABLE_1.getId(), TableState.TAKEN);
        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_ARRIVED));
        assertTableState(TABLE_2.getId(), TableState.TAKEN);

        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_DRINKS_DONE));
        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_DRINKS_DONE, "1100"));
        assertTableState(TABLE_2.getId(), TableState.DRINKING, 10, 100);

        assertTableState(TABLE_1.getId(), TableState.TAKEN);
        assertTableState(TABLE_1.getId(), TableState.DRINKING, 1_000, 1_500);
    }

    @Test
    @Order(6)
    @DisplayName("2 Bartenders - 3 Tables - 2 bartenders are blocked by the first 2 tables, 3rd table has to wait")
    void test_2_bartenders_3_tables_exactly_2_bartenders_drinking() {
        initAndAssertInit(2, 3);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_ARRIVED));
        assertTableState(TABLE_1.getId(), TableState.TAKEN);
        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_ARRIVED));
        assertTableState(TABLE_2.getId(), TableState.TAKEN);
        emitEvent(Event.of(TABLE_3.getId(), EventType.GUESTS_ARRIVED));
        assertTableState(TABLE_3.getId(), TableState.TAKEN);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_DRINKS_DONE, "1000"));
        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_DRINKS_DONE, "1000"));
        emitEvent(Event.of(TABLE_3.getId(), EventType.GUESTS_DRINKS_DONE));

        assertTableState(TABLE_3.getId(), TableState.TAKEN);
        assertTableState(TABLE_3.getId(), TableState.DRINKING, 1_000, 2_000);
    }

    @Test
    @Order(7)
    @DisplayName("1 Bartender - 2 Tables - 1 bartender takes care of arriving guests first then the rest")
    void test_1_bartender_2_tables_exactly_2_bartenders_arrived_drinking_left() {
        initAndAssertInit(1, 2);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_ARRIVED, "1000"));
        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_ARRIVED, "100"));

        assertTableState(TABLE_1.getId(), TableState.EMPTY);
        assertTableState(TABLE_1.getId(), TableState.TAKEN, 1_000, 2_000);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_DRINKS_DONE, "500"));

        assertTableState(TABLE_1.getId(), TableState.DRINKING, 500, 1_000);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_LEFT, "800"));

        assertTableState(TABLE_2.getId(), TableState.TAKEN, 100, 500);

        assertTableState(TABLE_1.getId(), TableState.EMPTY, 1_300, 2_000);
    }

    @Test
    @Order(8)
    @DisplayName("1 Bartender - 1 Table - Guests with leave limit")
    void test_1_bartender_1_table_guests_failing_to_leave_after_kicked_out() {
        initAndAssertInit(1, 1);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_ARRIVED, "{\"duration\": 200, \"mustLeaveIn\": 500}"));

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_DRINKS_DONE, "100"));

        await()
          .atMost(Duration.ofMillis(2_000))
          .pollDelay(Duration.ofMillis(10))
          .pollInterval(Duration.ofMillis(10))
          .until(() -> isEventEmitted(TABLE_1.getId(), EventType.GUESTS_KICKED_OUT, "timeout"));

        assertEquals(TableState.EMPTY, pubService
          .tables
          .get(1)
          .getState()
        );
    }

    @Test
    @Order(9)
    @DisplayName("1 Bartender - 1 Table - Invalid GUESTS_ARRIVING payload")
    void test_invalid_payload_emitted() {
        initAndAssertInit(1, 1);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_ARRIVED, "abc"));

        await()
          .atMost(Duration.ofMillis(2_000))
          .pollDelay(Duration.ofMillis(10))
          .pollInterval(Duration.ofMillis(10))
          .until(() -> isEventEmitted(TABLE_1.getId(), EventType.WRONG_ORDER_DETAILS, "GUESTS_ARRIVING"));
    }

    @Test
    @Order(10)
    @DisplayName("1 Bartender - 1 Table - Invalid payload in all states doesn't block the flow")
    void test_1_bartender_1_table_invalid_payload_emitted_complete_workflow() {
        initAndAssertInit(1, 1);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_ARRIVED, "invalid"));

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_DRINKS_DONE, "invalid"));

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_LEFT, "invalid"));

        await()
          .atMost(Duration.ofMillis(1_000))
          .pollDelay(Duration.ofMillis(10))
          .until(() ->
            isEventEmitted(TABLE_1.getId(), EventType.WRONG_ORDER_DETAILS, "GUESTS_ARRIVED") &&
              isEventEmitted(TABLE_1.getId(), EventType.WRONG_ORDER_DETAILS, "GUESTS_DRINKS_DONE") &&
              isEventEmitted(TABLE_1.getId(), EventType.WRONG_ORDER_DETAILS, "GUESTS_LEFT") &&
              pubService.emittedEvents.size() == 3);
    }

    @Test
    @Order(12)
    @DisplayName("2 Bartenders - 3 Tables - VIP customer arrived")
    void test_2_bartenders_3_tables_vip_customer_arrived() {
        initAndAssertInit(2, 3);

        final String name = "Józsi Bácsi";

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_ARRIVED));
        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_ARRIVED));
        emitEvent(Event.of(TABLE_3.getId(), EventType.GUESTS_ARRIVED));

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_DRINKS_DONE));
        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_DRINKS_DONE));
        emitEvent(Event.of(TABLE_3.getId(), EventType.GUESTS_DRINKS_DONE));

        emitEvent(Event.of(TABLE_2.getId(), EventType.VIP_GUEST_ARRIVED, name));

        await()
          .atMost(Duration.ofMillis(2_000))
          .pollInterval(Duration.ofMillis(10))
          .pollDelay(Duration.ofMillis(10))
          .until(() ->
            isEventEmitted(TABLE_2.getId(), EventType.GUESTS_KICKED_OUT, name) &&
            pubService
              .tables
              .get(TABLE_2.getId())
              .getState() == TableState.TAKEN);
    }

    @Test
    @Order(13)
    @DisplayName("2 Bartenders - 2 Tables - Guests drinking another round - Correct order payload")
    void test_2_bartenders_2_tables_guests_drinking_another_round_valid_payload() {
        initAndAssertInit(2, 3);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_ARRIVED));
        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_ARRIVED));

        assertTableState(TABLE_1.getId(), TableState.TAKEN);
        assertTableState(TABLE_2.getId(), TableState.TAKEN);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_DRINKS_DONE));
        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_DRINKS_DONE));

        assertTableState(TABLE_1.getId(), TableState.DRINKING);
        assertTableState(TABLE_2.getId(), TableState.DRINKING);

        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_DRINKS_DONE));

        assertTableState(TABLE_2.getId(), TableState.DRINKING);
    }

    @Test
    @Order(14)
    @DisplayName("2 Bartenders - 2 Tables - Guests drinking another round - Invalid order payload")
    void test_2_bartenders_2_tables_guests_drinking_another_round_invalid_payload() {
        initAndAssertInit(2, 3);

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_ARRIVED));
        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_ARRIVED));

        emitEvent(Event.of(TABLE_1.getId(), EventType.GUESTS_DRINKS_DONE));
        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_DRINKS_DONE, "invalid"));

        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) { }

        emitEvent(Event.of(TABLE_2.getId(), EventType.GUESTS_DRINKS_DONE, "invalid"));

        await()
          .atMost(Duration.ofMillis(1_000))
          .pollInterval(Duration.ofMillis(10))
          .pollDelay(Duration.ofMillis(10))
          .until(() ->
            isEventEmitted(TABLE_2.getId(), EventType.WRONG_ORDER_DETAILS, "GUESTS_DRINKS_DONE", 2) &&
              pubService
                .tables
                .get(TABLE_2.getId())
                .getState() == TableState.DRINKING);
    }

    @Test
    @Order(1000)
    @DisplayName("Optional test: 100 Bartenders - 100 000 Tables - Simple flow, performance should be below 1 sec")
    void test_100_bartenders_100000_tables_simpleWorkflowMustRunUnder1Sec() {
        initAndAssertInit(100, 100_000);

        IntStream.range(1, 100_001)
          .forEach(tableId ->
            emitEvent(Event.of(tableId, EventType.GUESTS_ARRIVED))
          );

        assertTableState(1, 100_000, TableState.TAKEN, 300);

        IntStream.range(1, 100_001)
          .forEach(tableId ->
            emitEvent(Event.of(tableId, EventType.GUESTS_DRINKS_DONE))
          );

        assertTableState(1, 100_000, TableState.DRINKING, 300);

        IntStream.range(1, 100_001)
          .forEach(tableId ->
            emitEvent(Event.of(tableId, EventType.GUESTS_LEFT))
          );

        assertTableState(1, 100_000, TableState.EMPTY, 300);
    }


    private void assertTableState(int fromTableId, int toTableId, TableState tableState, long maxMillis) {
        await()
          .atMost(Duration.ofMillis(maxMillis))
          .pollDelay(Duration.ofMillis(1))
          .pollInterval(Duration.ofMillis(1))
          .until(() ->
            IntStream.range(fromTableId, toTableId + 1)
              .allMatch(e -> pubService
                .tables
                .get(e)
                .getState() == tableState)
          );
    }

    private boolean isEventEmitted(int tableId, EventType eventType, String payload) {
        return isEventEmitted(tableId, eventType, payload, 1);
    }

    private boolean isEventEmitted(int tableId, EventType eventType, String payload, int emitCount) {
        return pubService
          .emittedEvents
          .stream()
          .filter(e -> e.getTableId() == tableId && e.getType() == eventType && (
            payload == null || payload.equals(e.getPayload())
          ))
          .count() == emitCount;
    }

    private void initAndAssertInit(int bartenderCount, int tableCount) {
        pubManager.init(bartenderCount, tableCount);
        await()
          .atMost(Duration.ofSeconds(1))
          .until(pubService::isRunning);
    }


    private void assertTableState(int tableId, TableState tableState) {
        assertTableState(tableId, tableState, 0, 1_000);
    }

    private void assertTableState(int tableId, TableState tableState, int atLeastMillis, int atMostMillis) {
        long start = System.currentTimeMillis();

        await()
          .pollDelay(Duration.ofMillis(atLeastMillis))
          .pollInterval(Duration.ofMillis(5))
          .atMost(Duration.ofMillis(atMostMillis))
          .until(() -> pubService
            .tables
            .values()
            .stream()
            .map(t -> t.getId() == tableId && t.getState() == tableState)
            .filter(e -> e)
            .findFirst()
            .orElse(false));

        long runtime = System.currentTimeMillis() - start;

        assertAll(
          () -> assertTrue(atLeastMillis <= runtime, assertMessageSupplier(tableId, tableState, atLeastMillis, runtime, atMostMillis)),
          () -> assertTrue(runtime <= atMostMillis, assertMessageSupplier(tableId, tableState, atLeastMillis, runtime, atMostMillis))
        );
    }

    private static Supplier<String> assertMessageSupplier(int tableId, TableState tableState, long atLeast, long runtime, long atMost) {
        return () -> "Table: " + tableId + ", didn't switch state to: " + tableState
          + " in the given " + atLeast + " ms <-> " + atMost + " ms timeframe, actually took " + runtime + " ms";
    }

    private void emitEvent(Event event) {
        pubService.eventListeners
          .entrySet()
          .stream()
          .filter(e -> e.getKey() == event.getType())
          .forEach(e -> {
              e.getValue().accept(event);
          });
    }


}
