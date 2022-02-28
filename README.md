# Building a pub/sub service in Go

In-process PubSub Implementation in Golang.

## Running the example

```shell
❯ GO111MODULE=off go run main.go
```

```
208B51C5-1F40B37F Subscribed for topic: BITCOIN
208B51C5-1F40B37F Subscribed for topic: ETHEREUM
60466C8A-3662A48A Subscribed for topic: ETHEREUM
60466C8A-3662A48A Subscribed for topic: SOLANA
Subscriber 60466C8A-3662A48A, received: 0.940509 from topic: ETHEREUM
Subscriber 208B51C5-1F40B37F, received: 0.940509 from topic: ETHEREUM
60466C8A-3662A48A Subscribed for topic: POLKADOT
Subscriber 60466C8A-3662A48A, received: 0.424637 from topic: SOLANA
60466C8A-3662A48A Unsubscribed for topic: SOLANA
Total subscribers for topic ETH is 2
Subscriber 208B51C5-1F40B37F, received: 0.515213 from topic: BITCOIN
Subscriber 60466C8A-3662A48A, received: 0.156519 from topic: ETHEREUM
Subscriber 208B51C5-1F40B37F, received: 0.156519 from topic: ETHEREUM
Subscriber 60466C8A-3662A48A, received: 0.283034 from topic: POLKADOT
Subscriber 60466C8A-3662A48A, received: 0.380657 from topic: POLKADOT
Subscriber 60466C8A-3662A48A, received: 0.218553 from topic: ETHEREUM
Subscriber 208B51C5-1F40B37F, received: 0.218553 from topic: ETHEREUM
60466C8A-3662A48A Unsubscribed for topic: ETHEREUM
60466C8A-3662A48A Unsubscribed for topic: POLKADOT
Total subscribers for topic ETH is 1
Subscriber 208B51C5-1F40B37F, received: 0.865335 from topic: BITCOIN
Subscriber 208B51C5-1F40B37F, received: 0.028303 from topic: ETHEREUM
Subscriber 208B51C5-1F40B37F, received: 0.059121 from topic: ETHEREUM
...
```

