# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
