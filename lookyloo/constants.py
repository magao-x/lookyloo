import os
import pathlib
import re

LOOKYLOO_DATA_ROOTS = os.environ.get(
    "LOOKYLOO_DATA_PATHS", "/opt/MagAOX:/srv/icc/opt/MagAOX:/srv/rtc/opt/MagAOX"
)
QUICKLOOK_PATH = pathlib.Path("/home/guestobs/obs")
LOG_PATH = pathlib.Path("/opt/MagAOX/logs/lookyloo")
HISTORY_FILENAME = ".lookyloo_succeeded"
FAILED_HISTORY_FILENAME = ".lookyloo_failed"
DEFAULT_SEPARATE = object()
DEFAULT_CUBE = object()
ALL_CAMERAS = {
    "camsci1": DEFAULT_SEPARATE,
    "camsci2": DEFAULT_SEPARATE,
    "camlowfs": DEFAULT_SEPARATE,
    "camwfs": DEFAULT_SEPARATE,
    "camtip": DEFAULT_SEPARATE,
    "camacq": DEFAULT_SEPARATE,
}
AUTO_EXPORT_CAMERAS = ["camsci1", "camsci2"]
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
FOLDER_TIMESTAMP_FORMAT = "%Y-%m-%d_%H%M%S"
