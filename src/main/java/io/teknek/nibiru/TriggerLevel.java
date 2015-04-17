package io.teknek.nibiru;

public enum TriggerLevel {
  /** Request will block while trigger is executing **/
  BLOCKING,
  /** Request will not block while trigger is executing. 
   * Triggers operations may be dropped if back pressure**/
  NON_BLOCKING_VOLATILE,
  /** Request will not block while trigger is executing. 
   * Trigger operations retry, potentially later */
  NON_BLOCKING_RETRYABLE
}
