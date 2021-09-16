# GAS Utilities
This directory contains the following utility-related files:
* `helpers.py` - Miscellaneous helper functions
* `util_config.py` - Common configuration options for all utilities

Each utility must be in its own sub-directory, along with its respective configuration file and run script, as follows:

/archive
* `archive_scipt.py` - Archives free user result files to Glacier using a script
* `archive_script_config.ini` - Configuration options for archive utility script
* `run_archive_scipt.sh` - Runs the archive script


/notify
* `notify.py` - Sends notification email on completion of annotation job
* `notify_config.ini` - Configuration options for notification utility

/restore
* `restore.py` - AWS Lambda code for restoring thawed objects to S3

/thaw
* `thaw_script.py` - Thaws an archived Glacier object using a script
* `thaw_script_config.ini` - Configuration options for thaw utility script
* `run_thaw_scipt.sh` - Runs the thaw script

If you completed Ex. 14, include your annotator load testing script here
* `ann_load.py` - Annotator load testing script
