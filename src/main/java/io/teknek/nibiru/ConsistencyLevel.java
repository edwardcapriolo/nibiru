package io.teknek.nibiru;

public enum ConsistencyLevel {

  /**
   * N replicas must respond before Timeout
   */
  N,
  /**
   * Implies that the master (source of truth) is where this must execute
   */
  MASTER,
  /**
   * Implies a slave (potentially out of sync) should be used to services this request
   */
  SLAVE,
  /**
   * Implies quorum of servers must respond in aggrrement before acknowledgement
   */
  QUORUM,
  /**
   * A quorum in the local datacenter
   */
  LOCAL_QUORUM,
  /**
   * All replicas must ack the request
   */
  ALL,
  /**
   * write only
   */
  ANY, SERIAL,
  /**
   * Do whatever the backend prefers or implies
   */
  IMPLIED,
  /**
   * //Implies a consistency level > one and some timeline of how long before accepting partial
   * results
   */
  DEADLINE,
  /**
   * write may be buffered into volatile storage and potentially lost
   */
  UNSAFE

}
