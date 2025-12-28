#!/usr/bin/env python
import os
import sys

from aggregator.core.management import execute_from_command_line


def main() -> None:
    os.environ.setdefault("AGGREGATOR_SETTINGS_MODULE", "aggregator.settings.base")
    if len(sys.argv) < 2:
        command = "run"
        args = []
    else:
        command = sys.argv[1]
        args = sys.argv[2:]

    execute_from_command_line(command, args)


if __name__ == "__main__":
    main()
