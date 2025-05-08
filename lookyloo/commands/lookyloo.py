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

from ..constants import (
    HISTORY_FILENAME,
    ALL_CAMERAS,
    AUTO_EXPORT_CAMERAS,
    LOOKYLOO_DATA_ROOTS,
    QUICKLOOK_PATH,
    DEFAULT_CUBE,
    DEFAULT_SEPARATE,
    CHECK_INTERVAL_SEC,
    LOG_PATH,
)
from ..utils import parse_iso_datetime, utcnow, format_timestamp_for_filename
from ..core import (
    TimestampedFile,
    ObservationSpan,
    load_file_history,
    do_quicklook_for_camera,
    get_new_observation_spans,
    process_span,
    decide_to_process,
)

log = logging.getLogger("lookyloo")


def main():
    now = datetime.datetime.now()
    this_year = now.year
    this_semester = str(this_year) + ("B" if now.month > 6 else "A")
    parser = argparse.ArgumentParser(description="Quicklookyloo")
    parser.add_argument(
        "-r",
        "--dry-run",
        help="Commands to run are printed in debug output (implies --verbose)",
        action="store_true",
    )
    parser.add_argument(
        "-i",
        "--ignore-history",
        help=f"When a history file ({HISTORY_FILENAME}) is found under the output directory, don't skip files listed in it",
        action="store_true",
    )
    parser.add_argument(
        "-C",
        "--cube-mode-all",
        help="Whether to write all archives as cubes, one per XRIF, regardless of the default for the device (implies --omit-telemetry)",
        action="store_true",
    )
    parser.add_argument(
        "-S",
        "--separate-mode-all",
        help="Whether to write all archives as separate FITS files regardless of the default for the device",
        action="store_true",
    )
    parser.add_argument(
        "-v", "--verbose", help="Turn on debug output", action="store_true"
    )
    parser.add_argument(
        "-t", "--title", help="Title of observation to collect", action="store"
    )
    parser.add_argument(
        "-e",
        "--observer-email",
        help="Skip observations that are not by this observer (matches substrings, case-independent)",
        action="store",
    )
    parser.add_argument(
        "-p",
        "--partial-match-ok",
        help="A partial match (title provided is found anywhere in recorded title) is processed",
        action="store_true",
    )
    parser.add_argument(
        "--find-partial-archives",
        help="When recording starts after stream-writing, archives may be missed. This option finds the last prior archive and exports it as well.",
        action="store_true",
    )

    parser.add_argument(
        "-s",
        "--semester",
        help=f"Semester to search in, default: {this_semester}",
        default=this_semester,
    )
    parser.add_argument(
        "--utc-start",
        help=f"ISO UTC datetime stamp of earliest observation start time to process (supersedes --semester)",
        type=parse_iso_datetime,
    )
    parser.add_argument(
        "--utc-end",
        help=f"ISO UTC datetime stamp of latest observation end time to process (ignored in daemon mode)",
        type=parse_iso_datetime,
    )

    parser.add_argument(
        "-c",
        "--camera",
        help=f"Camera name (i.e. rawimages subfolder name), repeat to specify multiple names. (default: {AUTO_EXPORT_CAMERAS})",
        action="append",
    )
    parser.add_argument(
        "-X",
        "--data-root",
        help=f"Search directory for telem and rawimages subdirectories, repeat to specify multiple roots. (default: {LOOKYLOO_DATA_ROOTS.split(':')})",
        action="append",
    )
    parser.add_argument(
        "-O",
        "--omit-telemetry",
        help="Whether to omit references to telemetry files",
        action="store_true",
    )
    parser.add_argument(
        "--ignore-data-integrity", help="[DEBUG USE ONLY]", action="store_true"
    )
    parser.add_argument(
        "-T",
        "--omit-symlink-tree",
        help="Whether to skip constructing the parallel structure of symlinks organizing observations by observer",
        action="store_true",
    )
    parser.add_argument(
        "--symlink-tree-dir",
        default=None,
        help="Root directory to construct symlink tree in (paths will have the form <symlink_tree_dir>/2023A/...), defaults to same as output dir",
    )
    parser.add_argument(
        "-D",
        "--output-dir",
        help=f"output directory, defaults to current dir",
        action="store",
        default=os.getcwd(),
    )
    parser.add_argument(
        "--xrif2fits-cmd",
        default="xrif2fits",
        help="Specify a path to an alternative version of xrif2fits here if desired",
        action="store",
    )
    parser.add_argument(
        "-j",
        "--parallel-jobs",
        default=8,
        type=int,
        help="Max number of parallel xrif2fits processes to launch (if the number of archives in an interval is smaller than this, fewer processes will be launched)",
    )
    args = parser.parse_args()
    output_path = pathlib.Path(args.output_dir)
    if not output_path.is_dir():
        output_path.mkdir(parents=True, exist_ok=True)

    if args.omit_symlink_tree:
        symlink_tree_dir = None
        log.debug("Omitting construction of symbolic link structure")
    else:
        if args.symlink_tree_dir is None:
            symlink_tree_dir = output_path
        else:
            symlink_tree_dir = pathlib.Path(args.symlink_tree_dir)
        log.debug(
            f"Creating a tree of symbolic links in {symlink_tree_dir} to organize outputs"
        )
    if symlink_tree_dir is not None and not symlink_tree_dir.is_dir():
        symlink_tree_dir.mkdir(parents=True, exist_ok=True)

    timestamp_str = format_timestamp_for_filename(utcnow())
    log_file_path = (
        f"./lookyloo_{timestamp_str}.log" if args.verbose or args.dry_run else None
    )
    log_format = "%(filename)s:%(lineno)d: [%(levelname)s] %(message)s"
    logging.basicConfig(
        level="DEBUG" if args.verbose or args.dry_run else "INFO",
        filename=log_file_path,
        format=log_format,
    )
    # Specifying a filename results in no console output, so add it back
    if args.verbose or args.dry_run:
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        logging.getLogger("").addHandler(console)
        formatter = logging.Formatter(log_format)
        console.setFormatter(formatter)
        log.debug(f"Logging to {log_file_path}")

    if args.cube_mode_all and args.separate_mode_all:
        raise RuntimeError(
            "Got both --cube-mode-all and --separate-mode-all... which do you want?"
        )

    if args.camera is not None:
        cameras = args.camera
    else:
        cameras = AUTO_EXPORT_CAMERAS
    if args.data_root:
        data_roots = [pathlib.Path(x) for x in args.data_root]
    else:
        data_roots = [pathlib.Path(x) for x in LOOKYLOO_DATA_ROOTS.split(":")]
    output_dir = pathlib.Path(args.output_dir)
    all_processed_files = set()
    letter = args.semester[-1].upper()
    if args.utc_start is not None:
        start_dt = args.utc_start
        year = start_dt.year
        month = 1 if start_dt.month < 6 else 6
        semester_start_dt = datetime.datetime(year, month, 1)
        semester_start_dt = semester_start_dt.replace(tzinfo=timezone.utc)
    else:
        try:
            if len(args.semester) != 5 or args.semester[-1].upper() not in ["A", "B"]:
                raise ValueError()
            year = int(args.semester[:-1])
            month = 1 if letter == "A" else 6
            day = 15 if month == 6 else 1
        except ValueError:
            raise RuntimeError(
                f"Got {args.semester=} but need a 4 digit year + A or B (e.g. 2022A)"
            )
        semester_start_dt = datetime.datetime(year, month, 1)
        semester_start_dt = semester_start_dt.replace(tzinfo=timezone.utc)
        start_dt = semester_start_dt

    semester_end_dt = datetime.datetime(
        year=year + 1 if letter == "B" else year,
        month=1 if letter == "B" else 6,
        day=15 if letter == "A" else 1,
    ).replace(tzinfo=timezone.utc)
    if args.utc_end is not None:
        end_dt = args.utc_end
    else:
        end_dt = semester_end_dt
    new_observation_spans, _ = get_new_observation_spans(
        data_roots,
        set(),
        start_dt,
        end_dt,
        ignore_data_integrity=args.ignore_data_integrity,
    )
    if args.cube_mode_all:
        force_mode = DEFAULT_CUBE
    elif args.separate_mode_all:
        force_mode = DEFAULT_SEPARATE
    else:
        force_mode = None

    spans_by_begin_dt = [(x.begin, x) for x in new_observation_spans]
    spans_by_begin_dt.sort()
    with futures.ThreadPoolExecutor(max_workers=args.parallel_jobs) as threadpool:
        for _, span in spans_by_begin_dt:
            if decide_to_process(args, span):
                log.info(f"Observation interval to process: {span}")
                process_span(
                    span,
                    output_dir,
                    cameras,
                    data_roots,
                    args.omit_telemetry,
                    args.xrif2fits_cmd,
                    all_processed_files,
                    args.ignore_history,
                    threadpool,
                    args.dry_run,
                    symlink_tree_dir=symlink_tree_dir,
                    force_cube_or_separate=force_mode,
                    ignore_data_integrity=args.ignore_data_integrity,
                    find_partial_archives=args.find_partial_archives,
                )
