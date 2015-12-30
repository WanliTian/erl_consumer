
-record(location, {
    host :: inet:ip_address() | inet:hostname(),
    port :: inet:port_number(),
    ref  :: gen_tcp:socket() | pid()
}).

-record(anchor, {
    group_id  :: binary(),
    topic     :: binary(),
    partition :: integer()
}).

-record(conn_state,{
    bro           :: #location{},
    coor          :: #location{},
    anchor        :: #anchor{},
    offset=-1     :: integer(),
    messages=[]   :: list(),
    is_down=false :: boolean()
}).

-record(consumer_state, {
    anchor   :: #anchor{},
    location :: #location{},
    skip_n=0 :: integer()
}).
