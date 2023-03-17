import os
import os.path
import datetime
import pathlib
import logging
import argparse
import time
import typing
from datetime import timezone
from concurrent import futures

from ..constants import HISTORY_FILENAME, ALL_CAMERAS, LOOKYLOO_DATA_ROOTS, QUICKLOOK_PATH, DEFAULT_CUBE, DEFAULT_SEPARATE, CHECK_INTERVAL_SEC, LOG_PATH
from ..utils import parse_iso_datetime, utcnow
from ..core import (
    load_file_history, TimestampedFile, ObservationSpan, do_quicklook_for_camera, get_new_observation_spans,
    process_span,
)

log = logging.getLogger('lookyloo')


def daemon_mode(
    output_dir : pathlib.Path, cameras : typing.List[str],
    data_roots: typing.List[pathlib.Path], omit_telemetry : bool,
    construct_symlink_tree: bool,
    xrif2fits_cmd: str,
    start_dt: datetime.datetime,
    all_visited_files: typing.List[TimestampedFile],
    ignore_history: bool,
    executor : futures.ThreadPoolExecutor,
    dry_run : bool,
):
    existing_observation_spans = set()
    log.info(f"Started at {datetime.datetime.now().isoformat()}, looking for unprocessed observations since {start_dt}...")
    while True:
        start_time = time.time()
        try:
            result = get_new_observation_spans(data_roots, existing_observation_spans, start_dt)
            new_observation_spans : typing.List[ObservationSpan] = result[0]
            start_dt : datetime.datetime = result[1]
            spans_with_data = set()
            for span in new_observation_spans:
                process_span(span, output_dir, cameras, data_roots, omit_telemetry, construct_symlink_tree, xrif2fits_cmd, all_visited_files, ignore_history, executor, dry_run)
                if span.end is not None:
                    spans_with_data.add(span)
            existing_observation_spans = existing_observation_spans.union(spans_with_data)
            duration = time.time() - start_time
            log.debug(f"Took {duration} sec")
            if duration < CHECK_INTERVAL_SEC:
                time.sleep(CHECK_INTERVAL_SEC - duration)
        except KeyboardInterrupt:
            raise
        except Exception:
            log.exception(f"Poll for new images failed with exception")


def main():
    now = datetime.datetime.now()
    this_year = now.year
    this_semester = str(this_year) + ("B" if now.month > 6 else "A")
    parser = argparse.ArgumentParser(description="Quicklookyloo")
    parser.add_argument('-r', '--dry-run', help="Commands to run are printed in debug output (implies --verbose)", action='store_true')
    parser.add_argument('-i', '--ignore-history', help=f"When a history file ({HISTORY_FILENAME}) is found under the output directory, don't skip files listed in it", action='store_true')
    parser.add_argument('-v', '--verbose', help="Turn on debug output", action='store_true')
    parser.add_argument('-X', '--data-root', help=f"Search directory for telem and rawimages subdirectories, repeat to specify multiple roots. (default: {LOOKYLOO_DATA_ROOTS.split(':')})", action='append')
    parser.add_argument('-D', '--output-dir', help=f"output directory, defaults to {QUICKLOOK_PATH.as_posix()}", action='store', default=QUICKLOOK_PATH.as_posix())
    parser.add_argument('-L', '--log-dir', help=f"output directory, defaults to {LOG_PATH.as_posix()}", action='store', default=LOG_PATH.as_posix())
    parser.add_argument('-j', '--parallel-jobs', default=8, help="Max number of parallel xrif2fits processes to launch (if the number of archives in an interval is smaller than this, fewer processes will be launched)")
    args = parser.parse_args()
    output_path = pathlib.Path(args.output_dir)
    if not output_path.is_dir():
        output_path.mkdir(parents=True, exist_ok=True)
    if not os.path.isdir(args.log_dir):
        os.makedirs(args.log_dir, exist_ok=True)
    log_file_path = f"{args.log_dir}/lookyloo_{time.time()}.log" if args.verbose or args.dry_run else None
    log_format = '%(filename)s:%(lineno)d: [%(levelname)s] %(message)s'
    logging.basicConfig(
        level='DEBUG' if args.verbose or args.dry_run else 'INFO',
        filename=log_file_path,
        format=log_format
    )
    # Specifying a filename results in no console output, so add it back
    if args.verbose or args.dry_run:
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        logging.getLogger('').addHandler(console)
        formatter = logging.Formatter(log_format)
        console.setFormatter(formatter)
        log.debug(f"Logging to {log_file_path}")

    cameras = list(ALL_CAMERAS.keys())
    if args.data_root:
        data_roots = [pathlib.Path(x) for x in args.data_root]
    else:
        data_roots = [pathlib.Path(x) for x in LOOKYLOO_DATA_ROOTS.split(':')]
    output_dir = pathlib.Path(args.output_dir)
    all_processed_files = load_file_history(output_dir / HISTORY_FILENAME) if not args.ignore_history else set()
    if args.utc_start is not None:
        start_dt = args.utc_start
        year = start_dt.year
        month = 1 if start_dt.month < 6 else 6
        semester_start_dt = datetime.datetime(year, month, 1)
        semester_start_dt = semester_start_dt.replace(tzinfo=timezone.utc)
    else:
        letter = args.semester[-1].upper()
        try:

            if len(args.semester) != 5 or args.semester[-1].upper() not in ['A', 'B']:
                raise ValueError()
            year = int(args.semester[:-1])
            month = 1 if letter == 'A' else 6
            day = 15 if month == 6 else 1
        except ValueError:
            raise RuntimeError(f"Got {args.semester=} but need a 4 digit year + A or B (e.g. 2022A)")
        semester_start_dt = datetime.datetime(year, month, 1)
        semester_start_dt = semester_start_dt.replace(tzinfo=timezone.utc)
        start_dt = semester_start_dt
    threadpool = futures.ThreadPoolExecutor(max_workers=args.parallel_jobs)

    try:
        daemon_mode(
            output_dir,
            cameras,
            data_roots,
            args.omit_telemetry,
            not args.omit_symlink_tree,
            args.xrif2fits_cmd,
            start_dt,
            all_processed_files,
            ignore_history=args.ignore_history,
            executor=threadpool,
            dry_run=args.dry_run,
        )
    finally:
        threadpool.shutdown()