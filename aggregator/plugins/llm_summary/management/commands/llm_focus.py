import argparse
import sys

from aggregator.core.management import execute_from_command_line


def main():
    parser = argparse.ArgumentParser(description="Focused cross-platform summary for a topic.")
    parser.add_argument("query", nargs="+", help="Topic to analyze, e.g. 'learning Portuguese'")
    parser.add_argument("--period", default="last_90_days", choices=["last_month", "last_90_days", "last_12_months"])
    args = parser.parse_args()
    question = " ".join(args.query)
    execute_from_command_line("llm_focus", [question, args.period])


if __name__ == "__main__":
    main()
