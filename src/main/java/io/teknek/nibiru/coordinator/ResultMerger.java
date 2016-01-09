package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.transport.BaseMessage;
import io.teknek.nibiru.transport.Response;

import java.util.List;

public interface ResultMerger {
  Response merge (List<Response> responses, BaseMessage message);
}
