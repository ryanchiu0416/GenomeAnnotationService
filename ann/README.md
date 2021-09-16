## GAS Annotator
This directory should contain all annotator files:
* `annotator.py` - Annotator control script; spawns AnnTools runner (run.py)
* `run.py` - Runs AnnTools and updates environment on completion (This is inside the `anntools` folder)
* `ann_config.ini` - Common configuration options for annotator.py and run.py
* `run_ann.sh` - Script for running annotator.py (called via user data on instance launch)
