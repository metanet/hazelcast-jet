package com.hazelcast.jet.impl;

public enum JobRestartStrategy {

    IMMEDIATELY,

    WITH_SHORT_ROLLBACK,

    AFTER_SUCCESSFUL_SNAPSHOT

}
