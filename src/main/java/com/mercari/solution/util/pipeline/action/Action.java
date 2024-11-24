package com.mercari.solution.util.pipeline.action;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;

import java.io.Serializable;

public interface Action extends Serializable {

    void setup();
    MElement action();
    MElement action(MElement value);
    Schema getOutputSchema();

    enum Service {
        dataflow,
        bigquery;
    }

}
