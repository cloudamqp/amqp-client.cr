# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.5] - 2019-10-11

### Added
- Queue#subscribe and Channel#basic_consume can now be blocked (until canceled)

## [0.4.4] - 2019-09-30

### Fixes
- Crystal 0.31.x deprecated URI.unescape, replaced with URI.decode_www_form

## [0.4.3] - 2019-09-23

### Fixed
- basic_nack sends a Nack and not a reject, thank you @jgaskins for the PR
