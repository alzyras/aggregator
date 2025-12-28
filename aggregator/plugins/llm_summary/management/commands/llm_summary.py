import argparse
import sys

from aggregator.core.management import execute_from_command_line


def main():
    parser = argparse.ArgumentParser(description="Ask a question against the LLM summary context.")
    parser.add_argument("question", nargs="*", help="Question to ask")
    args = parser.parse_args()
    question = " ".join(args.question) if args.question else "Give me a progress summary."
    execute_from_command_line("llm_summary", [question])


if __name__ == "__main__":
    main()
