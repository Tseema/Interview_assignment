package io.seon.scoring.interview.work_here;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.seon.scoring.interview.do_not_touch.dto.TableState;
import io.seon.scoring.interview.do_not_touch.event.Event;
import io.seon.scoring.interview.do_not_touch.event.EventType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.seon.scoring.interview.do_not_touch.event.PubService;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.Integer.parseInt;
import static java.lang.Thread.sleep;


@Component
@RequiredArgsConstructor
public class PubManager {

    @Autowired
    private final PubService pubService;

    ExecutorService executor = null;

    //Running on the assumption that we have events here from a subscriber (kafka or any other listener)
    @Getter
    @Setter
    public Map<EventType, Event> eventListeners = Collections.synchronizedMap(new LinkedHashMap<>());

    public void init(int bartenderCount, int tableCount) {
        pubService.setupPub(tableCount);

        pubService.startService();


        //register the listeners
        //registerListeners(eventListeners);
        //assignTask(bartenderCount);
    }


    /*
        event handler - whenever there is a new incoming new event we call this event
        handler  assigntask(bartenderCount)
        For the test cases to run need a hook to this method assignTask(bartender count)
        after the events have emitted.

        currently the events are subscribed into a local Linkedmap -> eventListeners(line 41)
        Since the PubService.eventListeners is private .

       This would later call the PubService::registerListener to add the events to
       PubService.eventListeners map.

     */
    public void assignTask(int bartenderCount) {

        // work with the number of bartenders available
        executor = Executors.newFixedThreadPool(Math.min(bartenderCount, 100));

        executor.submit(this::bartenderAllocation);

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            System.out.println(" Unexpected termination" + e);
        }
        // executor.submit(()-> System.out.println(" try "));
        // bartenderAllocation();

    }

    public void bartenderAllocation() {
        // expectations that subscriber is always reading events and there is event to operate on
        if (!eventListeners.isEmpty() && eventListeners.size() > 0 && eventListeners != null) {
            eventListeners
                    .forEach((key, value) -> {

                        Optional<List<Integer>> tablesToServe = getTablesToServe(key);

                        /*
                            optional task:
                            if there is a VIP that ahs arrived then handle the event by shouting
                            out guest_kicked_out_event with same payload .
                            Following which the tables with same id needs to be cleared
                            and status of table for the vip set to TAKEN.
                         */
                        if (tablesToServe.isPresent() && key == EventType.VIP_GUEST_ARRIVED) {
                            Event newEvent = Event.of(value.getTableId(), EventType.GUESTS_KICKED_OUT,
                                    value.getPayload());
                            pubService.shout(newEvent);
                            emptyTablesForVIP(tablesToServe);
                            pubService.setTableState(value.getTableId(), TableState.TAKEN);
                        }
                        else if (tablesToServe.isPresent() && key == EventType.GUESTS_ARRIVED
                                && isJSONValid(value.getPayload()))
                        {
                                //code for kicking out guest if payload has the time set
                            try
                            {
                                final ObjectMapper mapper = new ObjectMapper();
                                JsonNode jsonNode = mapper.readTree(value.getPayload());
                                long milliSeconds = jsonNode.path("mustLeaveIn").asLong();
                                try {
                                    sleep(milliSeconds);

                                    Event newEvent = Event.of(value.getTableId(),EventType.WRONG_ORDER_DETAILS,"timeout");
                                    pubService.shout(newEvent);
                                    pubService.setTableState(value.getTableId(),TableState.EMPTY);

                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }

                            } catch (JacksonException e) {
                                System.out.println("Could not read the Json" + e);
                            }
                        }
                        else if (tablesToServe.isPresent())
                        {
                            updateTableStatus(key, tablesToServe.get());
                        }
                        else {
                            System.out.println("No tables to attend to at the moment");
                        }

                    });
        }

    }

    public static boolean isJSONValid(String jsonInString) {
        try
        {
            final ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readTree(jsonInString);
            return true;
        } catch (JacksonException e) {
            return false;
        }
    }


    private void emptyTablesForVIP(Optional<List<Integer>> tablesToServe) {

        List<Event> events = List.of();

        for (Integer i : tablesToServe.get()) {
             eventListeners
                    .values()
                    .stream()
                    .filter(event -> event.getTableId().equals(i))
                     .forEach(events::add);
        }

        //removing every remaining event from your event queue for the corresponding table
        for (Event event : events) {
            eventListeners.remove(event);
        }

    }

    private void updateTableStatus(EventType key, List<Integer> tablesToServe)
    {
        if(key == EventType.GUESTS_ARRIVED)
        {
            tablesToServe
                    .forEach(id -> pubService.setTableState(id, TableState.TAKEN));
        }
        else if (key == EventType.GUESTS_DRINKS_DONE)
        {
            //once the drinks are delivered the table state is changed to DRINKING
            tablesToServe
                    .forEach(id -> pubService.setTableState(id, TableState.DRINKING));
        }
        else if (key == EventType.GUESTS_LEFT)
        {
            //clear the tables once the guest leave #BestPubEver
            tablesToServe
                    .forEach(id -> pubService.setTableState(id, TableState.EMPTY));
        }
    }


    private Optional<List<Integer>> getTablesToServe(EventType type) {

        List<Integer> tablesToServe = new ArrayList<>();
        //from the list of events find all table id's to attend to for a given event type

       Map<Boolean, List<Event>> collect = eventListeners
                .entrySet()
                .stream()
                .filter(events -> events.getKey() == type)
                .map(Map.Entry::getValue)
                .filter(this::checkForNoiseInEvent)
                .collect(Collectors.partitioningBy(e->!e.getPayload().isEmpty()));

        /*
         if the payload has time in it have the execution delayed by
         corresponding time in milliseconds .
         That is to say , the bartender needs to stall for that
         amount of time before he serves the table .
         */
        collect.get(true)
                .forEach(event -> {
                    try {
                        int i = parseInt(event.getPayload());
                        sleep(i);
                    } catch (NumberFormatException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } );


        collect.get(false)
                .forEach(event -> {
                    tablesToServe.add(event.getTableId());
                } );

        return Optional.of(tablesToServe);
    }


    public boolean checkForNoiseInEvent(Event e){

        try
        {
            // Assumption : that only non integer payload value constitutes as NOISE
            /*
            this criteria can change based on what constitutes as noise like
            an empty payload etc
             */
            parseInt(e.getPayload());
            System.out.println(
                    e.getPayload() + " is a valid integer number");
        }
        catch (NumberFormatException ex) {
            Event newEvent = Event.of(e.getTableId(),EventType.WRONG_ORDER_DETAILS, String.valueOf(e.getType()));
            pubService.shout(newEvent);
            System.out.println(
                    e.getPayload() + " is not a valid integer number");
        }

        return  true;

    }



    /*private void freeBartenders(List<Integer> tablesToServe) {
        bartenders
                .values()
                .stream()
                .filter(bartender -> tablesToServe.contains(bartender.getTableId()))
                .forEach(bartender -> {
                    bartender.setTableId(0);
                    bartender.setState(BartenderState.AVAILABLE);

                });
    }

    private void assignBartender(int tableId) {
        bartenders
                .values()
                .stream()
                .filter(attenders-> attenders.getState() == BartenderState.AVAILABLE)
                .forEach(attender-> {
                            attender.setTableId(tableId);
                            attender.setState(BartenderState.BUSY);
                        }
                );
    }*/

     /* public void bartenderAllocation(EventType type, Event Event) {
        if(!eventListeners.isEmpty()){
            List<Integer> tablesToServe = getTablesToServe(eventListeners,type);

            //now assign attenders to all the tables that need drink to be served
            tablesToServe
                    .forEach(this::assignBartender);

            if(type == EventType.GUESTS_DRINKS_DONE) {
                //once the drinks are delivered the table state needs changed to DRINKING
                tablesToServe
                        .forEach(id -> pubService.setTableState(id, TableState.DRINKING));
            }
            else if(type == EventType.GUESTS_LEFT){
                //clear the tables once the guest leave #BestPubEver
                tablesToServe
                        .forEach(id -> pubService.setTableState(id, TableState.EMPTY));
            }

            // and the bartender needs to be released of his task
            freeBartenders(tablesToServe);
        }
    }*/


}
