# osd-scrub-least

Show the Ceph PGs whose **last deep scrub** is longest ago, and print the **primary OSD** (plus acting set).  
Useful for spotting partitions that have not been deep-scrubbed for a while.

## Features

- Parses both legacy `"YYYY-MM-DD HH:MM:SS[.us]"` and ISO-8601 timestamps with `Z`, `+HH:MM`, or `+HHMM`
- Works with `ceph pg ls -f json` and `ceph pg dump_json` across Ceph versions
- Deterministic sorting by oldest deep scrub first
- Option `-n <NUM>` to show the last N PGs (default: 5)
- `--debug` to show parsed epoch timestamps for verification

## Requirements

- Perl 5.26+
- Ceph CLI on PATH with permissions to query PGs
- Perl modules:
  - `Cpanel::JSON::XS`
  - `DateTime::Format::ISO8601`
  - `Try::Tiny`
  - `Getopt::Long` (core)

### Install on Debian/Ubuntu

~~~~bash
apt update
apt install -y libcpanel-json-xs-perl libdatetime-format-iso8601-perl libtry-tiny-perl
~~~~

Or via cpanm:

~~~~bash
cpanm Cpanel::JSON::XS DateTime::Format::ISO8601 Try::Tiny
~~~~

## Usage

~~~~bash
# default: show 5 oldest PGs
./osd-scrub-least.pl

# show 10 oldest
./osd-scrub-least.pl -n 10

# debug epoch parsing
./osd-scrub-least.pl --debug
~~~~

Example output:

~~~~text
Top 5 PGs by oldest deep scrub:

PGID         Primary    Acting               Last Deep Scrub
----------------------------------------------------------------------
1.2f         osd.7      7,19,3              2025-07-14T03:12:45.123456Z
2.a3         osd.11     11,8,5              2025-07-15T08:02:17.457221+0000
~~~~

## How it works

- The script collects JSON from `ceph pg ls -f json` (preferred) or `ceph pg dump_json` (fallback).
- It extracts the PG array in a version-agnostic way (`.pg_stats`, `.pg_map.pg_stats`, `.entries`, or top-level array).
- Field `last_deep_scrub_stamp` is normalized:
  - Space is turned into `T`
  - Timezone `+HHMM` is normalized to `+HH:MM`
  - Missing timezone defaults to `Z` (UTC)
- Timestamps are parsed to epoch seconds. Missing/unknown becomes `0` and sorts first.
- Rows are sorted by `ts` ascending, with tie-breakers on the original stamp and `pgid`.

## Exit codes

- `0` on success
- Non-zero when Ceph JSON cannot be read/parsed or no PGs are found

## Notes

- If all timestamps are shown in ascending PGID order, enable `--debug` and check epochs.  
  Usually this indicates a timestamp format that was not normalized. The script already handles `+HHMM` → `+HH:MM`, ISO `Z`, and space vs `T`.
- For machine-readable output (JSON/TSV), open an issue or extend the printing section.  
  The data is already in structured form in `@rows`.
