#!/usr/bin/env perl
use strict;
use warnings;
use feature 'say';

use Getopt::Long qw(:config no_auto_abbrev);
use Cpanel::JSON::XS qw(decode_json);
use DateTime::Format::ISO8601;
use Try::Tiny;

# -----------------------------------------------------------------------------
# CLI options
#   -n <NUM> : show the NUM oldest deep-scrub PGs (default: 5)
#   --debug  : print parsed epoch timestamps to stderr for verification
#   --help   : usage
# -----------------------------------------------------------------------------

my $n = 5;
my $help;
my $debug = 0;

Getopt::Long::GetOptions(
    'n=i'   => \$n,
    'help'  => \$help,
    'debug' => \$debug,
) or die "Invalid options. See --help.\n";

if ($help) {
    print <<"USAGE";
Show the PGs whose last deep scrub is longest ago.

Usage:
  $0 [-n NUM] [--debug] [--help]

Options:
  -n NUM     Show the NUM oldest PGs (default: 5)
  --debug    Print parsed epoch timestamps for each PG
  --help     Show this help
USAGE
    exit 0;
}

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

sub slurp_cmd {
    my @cmd = @_;
    my $out = qx{@cmd 2>/dev/null};
    return $? == 0 ? $out : undef;
}

# Robust timestamp parser:
#  - accepts "YYYY-MM-DD HH:MM:SS[.us]" (no TZ -> treat as UTC)
#  - accepts ISO-8601 with Z, +HH:MM, or +HHMM (normalized to +HH:MM)
# Returns epoch seconds (UTC); unknown/empty -> 0 (interpreted as "never")
sub parse_ts {
    my ($s) = @_;
    return 0 unless defined $s && length $s;
    return 0 if $s eq '0.000000';

    my $norm = $s;
    $norm =~ s/ /T/;                          # space → T
    $norm =~ s/([+\-]\d{2})(\d{2})$/$1:$2/;   # +HHMM → +HH:MM
    $norm .= 'Z' unless $norm =~ /(?:Z|[+\-]\d{2}:\d{2})$/;  # add Z if TZ missing

    my $epoch = try { DateTime::Format::ISO8601->parse_datetime($norm)->epoch }
                catch { 0 };

    return $epoch // 0;
}

# Extract the PG array across Ceph versions/commands
sub extract_pg_array {
    my ($root) = @_;
    return $root if ref($root) eq 'ARRAY';
    return $root->{pg_stats}         if ref($root->{pg_stats}) eq 'ARRAY';
    return $root->{pg_map}{pg_stats} if ref($root->{pg_map}) eq 'HASH'
                                     && ref($root->{pg_map}{pg_stats}) eq 'ARRAY';
    return $root->{entries}          if ref($root->{entries}) eq 'ARRAY';
    return [];
}

# Determine primary OSD robustly
sub primary_osd_of {
    my ($pg) = @_;
    return $pg->{acting_primary} if defined $pg->{acting_primary};
    return $pg->{up_primary}     if defined $pg->{up_primary};
    return $pg->{acting}[0]      if ref($pg->{acting}) eq 'ARRAY' && @{$pg->{acting}};
    return $pg->{up}[0]          if ref($pg->{up})     eq 'ARRAY' && @{$pg->{up}};
    return undef;
}

# Build acting set as comma-separated list
sub acting_set_of {
    my ($pg) = @_;
    my $a = (ref($pg->{acting}) eq 'ARRAY') ? $pg->{acting}
          : (ref($pg->{up})     eq 'ARRAY') ? $pg->{up}
          : [];
    return join(',', @$a);
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

# Prefer modern 'ceph pg ls -f json', fallback to 'ceph pg dump_json'
my $json = slurp_cmd(qw(ceph pg ls -f json))
        // slurp_cmd(qw(ceph pg dump_json))
        or die "Unable to read Ceph JSON\n";

my $root = eval { decode_json($json) }
    or die "JSON parse error: $@\n";

my $pgs = extract_pg_array($root);
@$pgs or die "No PGs found\n";

# Transform into rows
my @rows = map {
    my $pgid = $_->{pgid};
    my $last = $_->{last_deep_scrub_stamp} // 'never';
    my $ts   = parse_ts($last);
    {
        pgid    => $pgid,
        ts      => $ts,
        primary => primary_osd_of($_),
        acting  => acting_set_of($_),
        last    => $last,
    }
} @$pgs;

# Sort by oldest last-deep-scrub first; deterministic tie-breakers
@rows = sort {
       ($a->{ts} // 0)       <=> ($b->{ts} // 0)
    || ($a->{last} // '')    cmp  ($b->{last} // '')
    || ($a->{pgid} // '')    cmp  ($b->{pgid} // '')
} @rows;

die "No evaluatable PGs\n" unless @rows;

# Bound N
$n = @rows if $n > @rows;

# Output table
say sprintf "Top %d PGs by oldest deep scrub:\n", $n;
say sprintf "%-12s %-10s %-20s %s", "PGID", "Primary", "Acting", "Last Deep Scrub";
say "-" x 70;

for my $r (@rows[0 .. $n-1]) {
    printf "%-12s osd.%-8s %-20s %s\n",
        $r->{pgid},
        ($r->{primary} // 'unknown'),
        $r->{acting},
        ($r->{last} // 'never');
    printf STDERR "[debug] %s ts=%d\n", $r->{pgid}, $r->{ts} if $debug;
}
