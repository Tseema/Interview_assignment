package io.seon.scoring.interview.do_not_touch.dto;

import lombok.Getter;
import lombok.Setter;


@Getter
public class Table {

    private final int id;
    @Setter
    private TableState state;

    private Table(int id, TableState state) {
        this.id = id;
        this.state = state;
    }

    public static Table of(int id) {
        return new Table(id, TableState.EMPTY);
    }

}
