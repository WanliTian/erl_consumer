
%%Define kafka error macro
-define(NoError, 0). 
-define(Unknown, -1).
-define(OffsetOutOfRange, 1). 
-define(InvalidMessage, 2). 
-define(UnknownTopicOrPartition, 3). 
-define(InvalidMessageSize, 4). 
-define(LeaderNotAvailable, 5). 
-define(NotLeaderForPartition, 6). 
-define(RequestTimedOut, 7). 
-define(BrokerNotAvailable, 8). 
-define(ReplicaNotAvailable, 9). 
-define(MessageSizeTooLarge, 10).
-define(StaleControllerEpochCode, 11).
-define(OffsetMetadataTooLargeCode, 12).
-define(OffsetsLoadInProgressCode, 14).
-define(ConsumerCoordinatorNotAvailableCode, 15).
-define(NotCoordinatorForConsumerCode, 16).
