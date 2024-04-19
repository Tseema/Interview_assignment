package io.seon.scoring.interview.do_not_touch.event;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import lombok.Getter;
import org.springframework.stereotype.Service;

import io.seon.scoring.interview.do_not_touch.dto.Table;
import io.seon.scoring.interview.do_not_touch.dto.TableState;


@Service
public class PubService {

    @Getter
    final Map<Integer, Table> tables = Collections.synchronizedMap(new LinkedHashMap<>());
    private final AtomicInteger NEXT_TABLE_ID = new AtomicInteger(1);
    @Getter
    boolean isRunning = false;


    final Map<EventType, Consumer<Event>> eventListeners = Collections.synchronizedMap(new LinkedHashMap<>());

    final List<Event> emittedEvents = Collections.synchronizedList(new LinkedList<>());

    public void registerListener(EventType eventType, Consumer<Event> consumer) {
        eventListeners.put(eventType, consumer);
    }

    public void setTableState(int tableId, TableState tableState) throws InvalidStateSwitchException {
        Table table = tables.get(tableId);
        if (table != null) {
            if (tableState == TableState.EMPTY || TableState.canSwitchState(table.getState(), tableState)) {
                table.setState(tableState);
            } else {
                throw new InvalidStateSwitchException();
            }
        }
    }

    public void setupPub(int tableCount) {
        stopService();
        tables.clear();
        emittedEvents.clear();
        NEXT_TABLE_ID.set(1);
        clearListeners();

        IntStream.range(0, tableCount)
          .forEach(e -> {
              final int tableId = NEXT_TABLE_ID.getAndIncrement();
              tables.put(tableId, Table.of(tableId));
          });
    }

    public void startService() {
        isRunning = true;
    }

    public void stopService() {
        isRunning = false;
    }

    public void shout(Event event) {
        emittedEvents.add(event);
    }

    public void clearListeners() {
        eventListeners.clear();
    }


}
