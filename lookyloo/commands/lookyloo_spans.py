import time
import os
import typing
from concurrent import futures
import logging
import datetime
from datetime import timezone
import pathlib
import argparse
from ..constants import HISTORY_FILENAME, ALL_CAMERAS, LOOKYLOO_DATA_ROOTS, QUICKLOOK_PATH, DEFAULT_CUBE, DEFAULT_SEPARATE, CHECK_INTERVAL_SEC, LOG_PATH
from ..utils import parse_iso_datetime, format_timestamp_for_filename, utcnow, get_search_start_end_timestamps
from ..core import (
    TimestampedFile,
    ObservationSpan,
    load_file_history,
    do_quicklook_for_camera,
    get_new_observation_spans,
    process_span,
    decide_to_process,
    create_bundle_from_span,
)

from ..utils import parse_iso_datetime

log = logging.getLogger('lookyloo')

def main():
    now = datetime.datetime.now()
    this_year = now.year
    this_semester = str(this_year) + ("B" if now.month > 6 else "A")
    parser = argparse.ArgumentParser(description="Quicklookyloo")
    parser.add_argument('-v', '--verbose', help="Turn on debug output", action='store_true')
    parser.add_argument('-t', '--title', help="Title of observation to collect", action='store')
    parser.add_argument('-e', '--observer-email', help="Skip observations that are not by this observer (matches substrings, case-independent)", action='store')
    parser.add_argument('-p', '--partial-match-ok', help="A partial match (title provided is found anywhere in recorded title) is processed", action='store_true')
    parser.add_argument('-s', '--semester', help=f"Semester to search in, default: {this_semester}", default=this_semester)
    parser.add_argument('--utc-start', help=f"ISO UTC datetime stamp of earliest observation start time to process (supersedes --semester)", type=parse_iso_datetime)
    parser.add_argument('--utc-end', help=f"ISO UTC datetime stamp of latest observation end time to process (ignored in daemon mode)", type=parse_iso_datetime)
    parser.add_argument('-X', '--data-root', help=f"Search directory for telem and rawimages subdirectories, repeat to specify multiple roots. (default: {LOOKYLOO_DATA_ROOTS.split(':')})", action='append')
    parser.add_argument('--ignore-data-integrity', help="[DEBUG USE ONLY]", action='store_true')
    args = parser.parse_args()

    log_format = '%(filename)s:%(lineno)d: [%(levelname)s] %(message)s'
    logging.basicConfig(
        level='DEBUG' if args.verbose else 'WARNING',
        format=log_format
    )
    # Specifying a filename results in no console output, so add it back
    if args.verbose:
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        logging.getLogger('').addHandler(console)
        formatter = logging.Formatter(log_format)
        console.setFormatter(formatter)

    if args.data_root:
        data_roots = [pathlib.Path(x) for x in args.data_root]
    else:
        data_roots = [pathlib.Path(x) for x in LOOKYLOO_DATA_ROOTS.split(':')]
    start_dt, end_dt = get_search_start_end_timestamps(args.semester, args.utc_start, args.utc_end)
    new_observation_spans, _ = get_new_observation_spans(data_roots, set(), start_dt, end_dt, ignore_data_integrity=args.ignore_data_integrity)
    new_observation_spans = [(x.begin, x) for x in new_observation_spans]
    new_observation_spans.sort()
    new_observation_spans = [x[1] for x in new_observation_spans]
    for obs in new_observation_spans:
        start_ts = obs.begin.isoformat()
        if obs.end is None:
            end_ts = 'ongoing'
        else:
            end_ts = obs.end.isoformat()

        should_display = True

        if args.title is not None:
            if not args.partial_match_ok and obs.title.lower() != args.title.lower():
                should_display = False
            elif args.partial_match_ok and args.title.lower() not in obs.title.lower():
                should_display = False
        if args.observer_email is not None:
            if args.observer_email.lower() not in obs.email:
                should_display = False

        if should_display:
            print(f"{start_ts}\t{end_ts}\t{repr(obs.email)}\t{repr(obs.title)}")