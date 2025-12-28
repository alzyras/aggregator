import argparse
import sys

from aggregator.core.management import execute_from_command_line


def main():
    parser = argparse.ArgumentParser(description="Generate a progress summary without an explicit question.")
    parser.add_argument("--period", default="last_month", choices=["last_month", "last_90_days", "last_12_months"])
    args = parser.parse_args()
    execute_from_command_line("llm_progress", [args.period])


if __name__ == "__main__":
    main()
