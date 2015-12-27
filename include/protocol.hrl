%%Define kafka error macro     
-define(NoError,                              0).           
-define(Unknown,                             -1).
-define(OffsetOutOfRange,                     1).  
-define(InvalidMessage,                       2).    
-define(UnknownTopicOrPartition,              3). 
-define(InvalidMessageSize,                   4). 
-define(LeaderNotAvailable,                   5). 
-define(NotLeaderForPartition,                6). 
-define(RequestTimedOut,                      7).   
-define(BrokerNotAvailable,                   8). 
-define(ReplicaNotAvailable,                  9). 
-define(MessageSizeTooLarge,                 10).
-define(StaleControllerEpochCode,            11).
-define(OffsetMetadataTooLargeCode,          12).
-define(OffsetsLoadInProgressCode,           14).
-define(ConsumerCoordinatorNotAvailableCode, 15).
-define(NotCoordinatorForConsumerCode,       16).

%% define kafka protocol common params
-define(API_VERSION,                         0).
-define(MAX_BYTES,               (5*1024*1024)).
-define(MIN_BYTES,                           1).
-define(MAX_WAIT_TIME,                    1000).

%% define kafka protocol apikey
-define(FETCH_REQUEST,                       1).
-define(OFFSET_REQUEST,                      2).
-define(METADATA_REQUEST,                    3).
-define(OFFSET_COMMIT_REQUEST,               8).
-define(OFFSET_FETCH_REQUEST,                9).
-define(CONSUMER_METADATA_REQUEST,          10).

%% define kafka protocol request and response record
-record(metadata_req,{
    topics=[] :: list()
}).

-record(metadata_res, {
    brokers=[] :: list(),
    topics =[] :: list()
}).

-record(fetch_req, {
    topic_anchor_list=[] :: list()
}).

-record(topic_anchor, {
    topic :: binary(),
    partition_anchor_list=[] :: list()
}).

-record(partition_anchor, {
    partition :: integer(),
    offset    :: integer(),
    max_bytes=?MAX_BYTES :: integer()
}).

-record(fetch_res, {
}).

-record(broker, {
    id=0 :: integer(),
    host :: binary(),
    port :: integer()
}).

-record(topic, {
    name          :: binary(),
    error_code=0  :: integer(),
    partitions=[] :: list()
}).

-record(partition, {
    id           :: integer(),
    error_code=0 :: integer(), 
    leader       :: integer(),
    reps=[]      :: list(),
    isrs=[]      :: list()
}).
