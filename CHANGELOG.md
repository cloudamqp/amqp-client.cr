# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
