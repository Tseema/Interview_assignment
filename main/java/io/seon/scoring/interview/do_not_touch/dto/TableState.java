package io.seon.scoring.interview.do_not_touch.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;


@Getter
@AllArgsConstructor
public enum TableState {

    EMPTY(1),
    TAKEN(2),
    DRINKING(3);

    private final int precedence;

    public static boolean canSwitchState(TableState from, TableState to) {
        return from.precedence == to.precedence - 1;
    }

}
