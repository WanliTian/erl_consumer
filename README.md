# erl_consumer
kafka consumer erlang client

###Config

    kafka_brokers            %%used to define kafka brokers {ip, port}
    auto_offset_largest      %%default value is largest, you can find the specified meaning in kafka wiki page
    cluster_info             %%this defines the kafka consumers cluster info.
                             %%you can set different erlang vm to consumer diffrent topics or both
    msg_callback             %%this is the thing you should to do for each message fetched from kafka.
                             %%the default action is to lager:notice to file

###TODO
- [x] auto multiple erlang nodes coordinate partitions
- [x] auto offset reset
- [x] auto start/stop workers based on kafka broker down/up/add/remove/change
- [x] skip messages for partitions
- [x] change kafka brokers dynamicly
- [x] auto config reload
