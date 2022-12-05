# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.10] - 2022-12-05

### Added

- Can configure multiple TCP options, both TCP and internal buffer sizes
- Streams queue example

### Changed

- Default to 60s (before 15s) read timeout during connection establishment
- Cancel consumer if callback raises unexpected exception
- Augment Connection::ClosedException with host, user and vhost
- Don't try to reject messages coming to a deleted consumer (might have been a noack consumer)

### Fixed

- Don't flush socket when publishing when in transaction mode, greatly increasing Tx publishing speed
- Removed exchange_declare's exclusive parameter
- Match parameter names for inherited methods
- Close all consumers when closing Channel to prevent blocks
- Raise an exception if trying to call wait_for_confirm on a channel that isn't in confirm mode

## [1.0.9] - 2022-03-27

### Added

- A OpenSSL::SSL::Context::Client can be passed to AMQP::Client and will be reused for multiple connection, saving considerable amount of memory

## [1.0.8] - 2022-03-24

### Added

- Make it possible to disable TCP keepalive (by setting the values to 0)

## [1.0.7] - 2022-01-31

### Fixed

- VERSION is correct (not the version of the app the lib is included in)

### Changed

- Default connection_name to PROGRAM_NAME, for easier identifitcation in the server's connection listing

## [1.0.6] - 2022-01-13

### Fixed

- Restore AMQP::Client::Message, as a abstract struct for Deliver and Get messages
- Requeue messages received for cancelled consumers

### Changed

- Make basic_cancel no_wait by default, so that consumers with prefetch 0 won't block on it.

## [1.0.5] - 2022-01-11

### Fixed

- Memory usage per connection is down 95%
- Allow custom queue properties also for temporary queues (where name is empty)

### Added

- Log warning that user should increase prefetch limit, when the read loop have to wait to deliver messages to worker(s)
- Queue#unsubscribe now supports the no_wait argument
- Better API documentation

### Changed

- Blocking consumers can now also have multiple workers
- Better exception handling in blocking consumers
- Reduced cyclomatic complexity in many methods

## [1.0.4] - 2021-11-16

### Added

- Can configure tcp keepalive settings with ?tcp_keepalive=60:10:3, (idle, interval, count)
- Can configure tcp nodelay settings with ?tcp_nodelay
- Support for AMQP Transactions

### Fixed

- Each connection is now using 95% less memory, 37KB/connection

## [1.0.3] - 2021-10-08

### Fixed

- Handle CloseOk write errors
- Using a closed channel would break connection

## [1.0.2] - 2021-09-21

### Changed

- Raise ChannelClosedError on write if channel is closed

## [1.0.1] - 2021-04-01

### Fixed

- Avoid trying to cancel already closed consumers
- Version is reported correctly

## [1.0.0] - 2021-03-23

### Changed

- Crystal 1.0.0 compability

## [0.6.6] - 2021-02-26

### Fixed

- Expect FlowOk after sending/setting Flow mode

## [0.6.5] - 2021-02-23

### Added

- Support for websockets

## [0.6.4] - 2021-02-07

### Fixed

- AMQP::Client::Connection::ClosedException is not wrapped as AMQP::Client::Error

## [0.6.3] - 2021-02-02

### Added

- All exceptions raised in this lib do now inherit from AMQP::Client::Error

## [0.6.2] - 2021-01-27

### Changed

- Default connect timeout is increased to 15s
- No default write timeout (was 15s before)
- By default we set the connection name to the name of the executable

### Fixed

- Blocking consumers are closed when connection is closed by broker
- Update for crystal 0.36.0

## [0.6.1] - 2020-08-28

### Changed

- Doesn't try to declare the default exchange

### Added

- work_pool parameter to Channel#basic_consume and Queue#subscribe with how many fibers that should process incoming messages, default 1

## [0.6.0] - 2020-08-04

### Added

- Channel#basic_publish can now publish a Bytes array directly

### Changed

- Queue#unsubscribe and Channel#basic_cancel doesn't take the no_wait argument anymore
- By default wait for Connection::CloseOk before returning

### Fixed

- When a consumer is canceled, we wait for CancelOk before we delete the consumer handler

## [0.5.20] - 2020-06-11

### Added

- Connection#close now as a `no_wait` argument (default to true) to wait for CloseOk from the server

## [0.5.19] - 2020-06-11

### Fixed

- Faster deliveries, by removing Log.context.set

## [0.5.18] - 2020-05-06

### Changed

- Socket buffer size set to 16KB (up from 8KB)

### Added

- Can set connection name, via URL (?name=MyConn) or the parameter `name`

## [0.5.17] - 2020-05-05

### Changed

- Default channel_max is now 1024 (more than ~8000 channels will deplete the stack memory)
- Connection#channel can now accepts any type of Int

### Fixed

- Bug when opening many channels

## [0.5.16] - 2020-04-12

### Fixed

- Crystal 0.34 compability

### Changed

- Uses the new Log module instead of Logger

## [0.5.15] - 2020-04-07

### Fixed

- Don't try catch Errno, it's removed in crystal 0.34

## [0.5.14] - 2020-03-25

### Fixed

- Default to vhost '/' if path in URI is empty

## [0.5.13] - 2020-03-25

### Fixed

- Close socket if connection establishing fails

## [0.5.12] - 2020-03-24

### Fixed

- Connection#channel(&block) now properly closes the channel after the block returns

## [0.5.11] - 2020-03-23

### Changed

- Setting progname of Logger to amqp-client.cr

### Fixed

- read_loop doesn't log errors on connection close

## [0.5.10] - 2020-03-23

### Fixed

- Negotiate channel_max and frame_max correctly
- Multi threading safety, a write lock is used when sending frame

## [0.5.9] - 2020-03-19

### Fixed

- Prevent double close, closing a closed connection won't raise exception

## [0.5.8] - 2020-03-19

### Changed

- An exception is now raised when sending frames if the server closed the connection

## [0.5.7] - 2020-03-10

### Added

- Queue#message_count and Queue#consumer_count methods (does a passive declare)

### Fixed

- Hostname in the URI amqp:///vhost are replaced with localhost

## [0.5.6] - 2020-02-24

### Fixed

- Multi-threading safety

## [0.5.5] - 2020-02-13

### Fixed

- Channel#wait_for_confirm raises ClosedException if channel is closed when returning

## [0.5.4] - 2020-02-13

### Added

- Channel#on_confirm(msgid, &blk) calls the block when the specific message is confirmed

### Fixed

- wait_for_confirm is now fiber safe

## [0.5.3] - 2020-02-12

### Added

- You can now pass an URI to the constructor

### Changed

- ArgumentError if Channel#wait_for_confirm parameter is less than 1
- Don't log warning of server consumer cancellation if connection/channel is closed

## [0.5.2] - 2020-02-11

### Changed

- The logger writes to STDERR instead of STDOUT

## [0.5.1] - 2020-02-10

### Added

- Messages from Channel#basic_get now includes a message_count property (that is how many msgs are left in the queue)

## [0.5.0] - 2020-02-04

### Added

- Allow publishing IO objects and manually setting the bytesize (for IO objects that doesn't support IO#bytesize)

### Changed

- Message deliveries/returns are process in a separate fiber so not to block frame processing
- Deliveries, returns and confirms are now added to unbounded internal dequeues
- Connection and channel close now use reply code 200 and no reply text

## [0.4.5] - 2019-10-11

### Added

- Queue#subscribe and Channel#basic_consume can now be blocked (until canceled)

## [0.4.4] - 2019-09-30

### Fixes

- Crystal 0.31.x deprecated URI.unescape, replaced with URI.decode_www_form

## [0.4.3] - 2019-09-23

### Fixed

- basic_nack sends a Nack and not a reject, thank you @jgaskins for the PR
