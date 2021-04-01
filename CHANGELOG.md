# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
