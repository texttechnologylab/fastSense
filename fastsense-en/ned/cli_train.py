import argparse
import datetime
from ned.train import TrainJobRunner


def train():
	arg_parser = argparse.ArgumentParser()
	arg_parser.add_argument(
		"--data",
		type=str,
		required=True,
		help="Path to folder containing trainings and test data."
	)
	arg_parser.add_argument(
		"--models_dir",
		type=str,
		required=True,
		help="Path to folder containing models. You can continue training an existing model by using the same name."
	)
	arg_parser.add_argument(
		"--final_models_dir",
		type=str,
		default=None,
		required=False,
		help=(
			"Path to output folder. Each model will be placed in a timestamped subfolder. Parameters and test results "
			"will be written to 'info.json' inside that subfolder."
		)
	)
	arg_parser.add_argument(
		"--jobs",
		type=str,
		required=True,
		help=(
			"Path to JSON file containing list of jobs. A job is a dict with the following keys: dataset_name (string), "
			"model_name (string), params (dict), epochs (int), batch_size (int), and only_test (bool, optional)"
		)
	)
	args = arg_parser.parse_args()

	start_time = datetime.datetime.now()

	job_runner = TrainJobRunner(
		dataset_base_path=args.data,
		models_dir=args.models_dir,
		final_models_dir=args.final_models_dir
	)
	job_runner.load_jobs(args.jobs)
	job_runner.run()

	end_time = datetime.datetime.now()
	duration = (end_time - start_time).total_seconds()

	print("")
	print("Done! Duration: {:.1f} min".format(duration / 60.0))
