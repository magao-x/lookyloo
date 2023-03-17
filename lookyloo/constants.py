import pathlib
import os
import re

LOOKYLOO_DATA_ROOTS = os.environ.get('LOOKYLOO_DATA_PATHS', '/opt/MagAOX:/srv/icc/data:/srv/rtc/data')
QUICKLOOK_PATH = pathlib.Path('/data/obs')
LOG_PATH = pathlib.Path('/opt/MagAOX/logs/lookyloo')
HISTORY_FILENAME = "lookyloo_success.txt"
FAILED_HISTORY_FILENAME = "lookyloo_failed.txt"
DEFAULT_SEPARATE = object()
DEFAULT_CUBE = object()
ALL_CAMERAS = {
    'camsci1': DEFAULT_SEPARATE,
    'camsci2': DEFAULT_SEPARATE,
}
SLEEP_FOR_TELEMS = 5
CHECK_INTERVAL_SEC = 30
LINE_BUFFERED = 1
XRIF2FITS_TIMEOUT_SEC = 120

# note: we must truncate to microsecond precision due to limitations in
# `datetime`, so this pattern works only after chopping off the last
# three characters
MODIFIED_TIME_FORMAT = "%Y%m%d%H%M%S%f"
PRETTY_MODIFIED_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
OBSERVERS_DEVICE = "observers"
LINE_FORMAT_REGEX = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{6})(?:\d{3}) "
    r"TELM \[observer\] email: (.*) obs: (.*) (\d)"
)
FOLDER_TIMESTAMP_FORMAT = '%Y%m%dT%H%M%S'