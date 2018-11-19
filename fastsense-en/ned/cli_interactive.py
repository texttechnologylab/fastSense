import time
from ned.disambiguator import Disambiguator
from ned.corenlp import CoreNlpBridge
import argparse
import os
import sys


def main():
	arg_parser = argparse.ArgumentParser(description="Interactive word sense disambiguator.")
	arg_parser.add_argument("--corenlp", type=str, required=True, help="Path to folder containing CoreNLP")
	arg_parser.add_argument("--model", type=str, required=True, help="Path to model")
	args = arg_parser.parse_args()

	classpath = os.path.join(args.corenlp, "*")

	properties = {
		"annotators": "tokenize,ssplit,pos,lemma",
		"tokenize.options": "untokenizable=noneKeep,invertible=true,ptb3Escaping=false",
		"tokenize.language": "en"
	}

	if not sys.stdin.isatty():
		stdin_text = "\n".join(sys.stdin.readlines())
	else:
		stdin_text = None

	corenlp_bridge = CoreNlpBridge(classpath=classpath, properties=properties, process_count=1)

	try:
		with Disambiguator(model_path=args.model, corenlp_bridge=corenlp_bridge, worker_count=1) as disambiguator:
			print("")
			print("Press ^C or ^D to exit.")
			print("")

			while True:
				if stdin_text is not None:
					input_text = stdin_text
					stdin_text = None
				else:
					input_text = input("\033[0;35m" + ">" + "\033[m ")

				if len(input_text) == 0:
					continue

				start_time = time.perf_counter()
				results = disambiguator.disambiguate(input_text)
				duration = time.perf_counter() - start_time

				print("")

				if len(results) == 0:
					print("\033[4;31m" + "Found no ambiguous words." + "\033[0m")
				else:
					results = sorted(results, key=lambda x: (x[0], -x[1]))

					print("\033[4;32m" + "Found {:d} ambiguous words in {:.4f} sec:".format(
						len(results),
						duration
					) + "\033[0m")

					prev_range = (0, 0)
					for start, end, url in results:
						word_range = (start, end)
						word = input_text[start:end]

						if prev_range[0] <= word_range[0] <= word_range[1] <= prev_range[1]:
							# Phrase inside range of previous phrase
							prefix = "  +-> "
						else:
							prefix = "* "
							prev_range = word_range

						format_str = prefix + "\033[1m" + "{} " + "\033[2;34m" + "({:d}-{:d})" + "\033[0m" + " - " + "\033[4;34m" + "{}" + "\033[0m"
						print(format_str.format(word, word_range[0], word_range[1], url))

				print("")

	except KeyboardInterrupt:
		pass
	except EOFError:
		pass
	finally:
		print("\033[0m")
		print("Exiting...")


if __name__ == "__main__":
	main()
