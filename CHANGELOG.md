# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-02-04

### Changed
- Expose `peek_token` and `skip_value` publicly.
- Reset reader state after `skip_value` to keep `next_object_entry` consistent.

### Added
- Tests covering `skip_value` state reset and `peek_token` non-consuming behavior.

## [0.1.0] - 2026-01-20

### Added
- Initial async JSON stream reader ported from Extract.
- Token-based streaming API for selective parsing.
