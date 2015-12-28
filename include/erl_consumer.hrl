
-record(location, {
    host :: inet:ip_address() | inet:hostname(),
    port :: inet:port_number(),
    ref  :: gen_tcp:socket()
}).

-record(anchor, {
    group_id  :: binary(),
    topic     :: binary(),
    partition :: integer()
}).

-record(conn_state,{
    bro           :: #location{},
    coo           :: #location{}
    anchor        :: #anchor{},
    offset        :: integer(),
    is_down=false :: boolean()
}).
