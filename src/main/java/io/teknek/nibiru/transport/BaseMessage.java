package io.teknek.nibiru.transport;

import io.teknek.nibiru.transport.columnfamily.GetMessage;
import io.teknek.nibiru.transport.columnfamilyadmin.CleanupMessage;
import io.teknek.nibiru.transport.keyvalue.Get;
import io.teknek.nibiru.transport.keyvalue.Set;
import io.teknek.nibiru.transport.metadata.CreateOrUpdateKeyspace;
import io.teknek.nibiru.transport.metadata.CreateOrUpdateStore;
import io.teknek.nibiru.transport.metadata.GetKeyspaceMetaData;
import io.teknek.nibiru.transport.metadata.GetStoreMetaData;
import io.teknek.nibiru.transport.metadata.ListKeyspaces;
import io.teknek.nibiru.transport.metadata.ListLiveMembers;
import io.teknek.nibiru.transport.metadata.ListStores;
import io.teknek.nibiru.transport.rpc.BlockingRpc;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.annotate.JsonSubTypes.Type;


@JsonTypeInfo(  
        use = JsonTypeInfo.Id.CLASS,  
        include = JsonTypeInfo.As.PROPERTY,  
        property = "type") 

    @JsonSubTypes({
        //metadata
        @Type(value = ListLiveMembers.class, name = "ListLiveMembers"),
        @Type(value = CreateOrUpdateKeyspace.class, name = "CreateOrUpdateKeyspace"),
        @Type(value = GetStoreMetaData.class, name = "GetStoreMetaData"),
        @Type(value = ListStores.class, name = "ListStores"),
        @Type(value = ListKeyspaces.class, name = "ListKeyspaces"),
        @Type(value = GetKeyspaceMetaData.class, name = "GetKeyspaceMetaData"),
        @Type(value = CreateOrUpdateStore.class, name = "CreateOrUpdateStore"),
        //keyvalue
        @Type(value = Set.class, name = "Set"),
        @Type(value = Get.class, name = "Get"),
        //rpc
        @Type(value = BlockingRpc.class, name = "BlockingRpc"),
        //columnfamily
        @Type(value = GetMessage.class, name = "GetMessage"),
        //columnfamilyadmin
        @Type(value = CleanupMessage.class, name = "CleanupMessage"),
         })
public class BaseMessage {

}
