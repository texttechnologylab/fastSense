from setuptools import setup

setup(
	name="ned",
	version="1.0",
	description="Named Entity Disambiguation",
	author="Clemens Schulz",
	packages=["ned", "ned.wiki"],
	install_requires=[
		"tensorflow",
		"numpy",
		"pyjnius",
		"ftfy",
		"mwparserfromhell"
	],
	entry_points={
		"console_scripts": [
			'ned = ned.cli_interactive:main',
			'ned-wiki-prepare = ned.cli_wiki:prepare',
			'ned-wiki-export = ned.cli_wiki:export',
			'ned-train = ned.cli_train:train'
		]
	}
)
