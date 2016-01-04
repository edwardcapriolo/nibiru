package io.teknek.nibiru.transport;

import io.teknek.nibiru.transport.rpc.BlockingRpcResponse;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.annotate.JsonSubTypes.Type;

@JsonTypeInfo(  
        use = JsonTypeInfo.Id.CLASS,  
        include = JsonTypeInfo.As.PROPERTY,  
        property = "type") 

    @JsonSubTypes({

        //rpc
        @Type(value = BlockingRpcResponse.class, name = "BlockingRpcResponse"),
        @Type(value = Response.class, name = "Response"),
         })
public interface BaseResponse {

}
