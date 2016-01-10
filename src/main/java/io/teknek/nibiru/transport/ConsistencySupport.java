package io.teknek.nibiru.transport;

import io.teknek.nibiru.Consistency;

public interface ConsistencySupport {
  Consistency getConsistency();
  void setTimeout(Long timeout);
  Long getTimeout();
}
