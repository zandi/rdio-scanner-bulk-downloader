#!/usr/bin/env python3

import argparse
import datetime
import glob
import json
import os

from tqdm import trange
from websockets.sync.client import connect

def get_talkgroups_from_config(server_config):
	'''
	Given a parsed config from a rdio-scanner server, iterate over
	the systems extracting just the basic talkgroup information for later
	use for quick lookup of talkgroups by name/id for filtering.

	returns (full list of talkgroups, dict of id to label, dict of label to id)
	'''
	systems_root = server_config[1]['systems']
	tg_label_to_id = {}
	tg_id_to_label = {}
	talkgroups = {}
	for sys in systems_root:
		for tg in sys['talkgroups']:
			tg_label_to_id[tg['label']] = tg['id']
			tg_id_to_label[tg['id']] = tg['label']
			tg['system'] = sys['id']
			talkgroups[tg['id']] = tg

	return (talkgroups, tg_id_to_label, tg_label_to_id)

def get_version_and_config(ws):
	version_request = json.dumps(["VER"])
	ws.send(version_request)
	message = ws.recv()
	server_version = json.loads(message)

	config_request = json.dumps(["CFG"])
	ws.send(config_request)
	message = ws.recv()
	server_config = json.loads(message)

	return (server_version, server_config)

def fetch_call_meta_list(ws, talkgroup, begin_date, end_date):
	'''
	fetch all calls from talkgroup, within date range specified by begin_date and end_date.
	talkgroup is an enriched talkgroup object, containing the system id.

	returns the full list of matching calls
	'''
	before_end_date_results = []
	after_begin_date_results = []

	# generic query that we mutate
	query_obj = ["LCL", {"limit": 200, "offset": 0, "system": talkgroup['system'], "talkgroup": talkgroup['id']}]

	# sort: -1 => everything before given datetime (default)
	# sort: 1 => everything after given datetime
	query_obj[1]['sort'] = -1
	query_obj[1]['date'] = end_date

	# do first query, calls before end datetime
	query = json.dumps(query_obj)
	ws.send(query)
	response = ws.recv()
	response_obj = json.loads(response)
	while response_obj[0] != 'LCL':
		# janky handling of spurious 'LSC' responses. just re-recv
		response = ws.recv()
		response_obj = json.loads(response)


	full_count = response_obj[1]["count"]
	results = response_obj[1]["results"]
	before_end_date_results.extend(results)
	#print(f"query response: limit={query_obj[1]['limit']}, {full_count=}, results: {len(results)}")

	# dumb approach: if we have as many results as is our limit, up the offset by the limit and run again
	# would be more technically correct to count results, but this seems easier
	while len(results) == query_obj[1]['limit']:
		#print("more calls remaining")
		query_obj[1]['offset'] += query_obj[1]['limit']

		query = json.dumps(query_obj)
		#print(f"{query=}")
		ws.send(query)
		response = ws.recv()
		response_obj = json.loads(response)
		while response_obj[0] != 'LCL':
			# janky handling of spurious 'LSC' responses. just re-recv
			response = ws.recv()
			response_obj = json.loads(response)

		results = response_obj[1]["results"]
		before_end_date_results.extend(results)
		#print(f"query response: limit={query_obj[1]['limit']}, {full_count=}, results: {len(results)}")

	# now all the calls after an end datetime
	query_obj[1]['sort'] = 1
	query_obj[1]['date'] = begin_date
	query_obj[1]['offset'] = 0

	# do first query, calls after begin datetime
	query = json.dumps(query_obj)
	ws.send(query)
	response = ws.recv()
	response_obj = json.loads(response)
	while response_obj[0] != 'LCL':
		# janky handling of spurious 'LSC' responses. just re-recv
		response = ws.recv()
		response_obj = json.loads(response)

	full_count = response_obj[1]["count"]
	results = response_obj[1]["results"]
	after_begin_date_results.extend(results)
	#print(f"query response: limit={query_obj[1]['limit']}, {full_count=}, results: {len(results)}")

	while len(results) == query_obj[1]['limit']:
		#print("more calls remaining")
		query_obj[1]['offset'] += query_obj[1]['limit']

		query = json.dumps(query_obj)
		#print(f"{query=}")
		ws.send(query)
		response = ws.recv()
		response_obj = json.loads(response)
		while response_obj[0] != 'LCL':
			# janky handling of spurious 'LSC' responses. just re-recv
			response = ws.recv()
			response_obj = json.loads(response)

		results = response_obj[1]["results"]
		after_begin_date_results.extend(results)
		#print(f"query response: limit={query_obj[1]['limit']}, {full_count=}, results: {len(results)}")
		pass

	#print(f"total before_end results: {len(before_end_date_results)}")
	#print(f"total after_begin results: {len(after_begin_date_results)}")

	# TODO: merge result sets to get intersection. Use the call ids
	# iterate over each list creating a two sets of ids. take the intersection to get
	# just the ids we want. Then, pull these full call details (if necessary?) from one of the results.
	# results are {id, dateTime, system, talkgroup}, where id is the unique identifier
	before_end_ids = {x['id'] for x in before_end_date_results}
	after_begin_ids = {x['id'] for x in after_begin_date_results}
	wanted_ids = before_end_ids & after_begin_ids

	# doesn't matter which result set we choose from, we're taking the intersection
	wanted_calls = [call for call in before_end_date_results if call['id'] in wanted_ids]

	return wanted_calls

def save_one_call(call_meta, call, out_dir):
	'''
	given call metadata and full call and output directory, save
	the call data to its corresponding location in the output directory.

	assumes that the necessary directory tree is already created (systems, talkgroups)
	'''
	call_path = os.path.join(out_dir, str(call_meta['system']), str(call_meta['talkgroup']))

	audio_data = bytes(call['audio']['data'])
	audio_filename = call['audioName']
	full_filename = os.path.join(call_path, audio_filename)

	#print(f"saving audio to {full_filename}")
	with open(full_filename, "wb") as f:
		f.write(audio_data)

	return

def download_one_call(ws, call_id):
	'''
	download one call from the rdio-scanner server.
	`ws`: websocket to use
	`call_id`: numeric id of the call

	returns full resulting JSON object
	'''
	query_obj = ['CAL', call_id, 'd']
	query = json.dumps(query_obj)
	ws.send(query)
	response = ws.recv()
	response_obj = json.loads(response)
	while response_obj[0] != 'CAL':
		# sometimes we get a spurious 'LSC' response from the server, apparently
		# listened count. We don't care about this, but should still get a 'CAL'
		# response from our 'CAL' request.
		#print(f"received unexpected response type {response_obj[0]}, calling recv")
		response = ws.recv()
		response_obj = json.loads(response)

	return response_obj

def resume_batch_download(outdir):
	'''
	read the config and progress files in the outdir, resume the configured batch download
	from where we left off (trying the call index in the progress file)

	'''
	# re-load configuration & state from file
	config_glob_path = os.path.join(outdir, '*.config')
	progress_file_path = os.path.join(outdir, 'progress-index.txt')
	configs = glob.glob(config_glob_path)

	config = {}
	with open(configs[0], 'r') as f:
		config = json.loads(f.read())

	args = config['args']
	calls_meta = config['call_metadata_list']

	with open(progress_file_path, 'r') as f:
		cur_index = int(f.read())

	print(f"resuming batch download {args}")
	print(f"currently restarting at call {cur_index} of {len(calls_meta)}")

	with connect(args['uri']) as ws:
		# download the calls, going in the same order as the call_meta list, saving our full list and progress
		# after each successful call downloads.
		for i in trange(cur_index, len(calls_meta)):
			with open(progress_file_path, 'w') as f:
				f.write( str(i) )

			call_meta = calls_meta[i]
			call_path = os.path.join(outdir, str(call_meta['system']), str(call_meta['talkgroup']))

			#print(f"downloading call {call_meta}")
			result = download_one_call(ws, call_meta['id'])
			if result[0] == "CAL":
				call = result[1]
				save_one_call(call_meta, call, outdir)
			else:
				print(f"error: unexpected response: {result}")
				quit()

	with open(progress_file_path, 'w') as f:
		f.write("done")

	return

def download_calls_new(ws, calls_meta, args):
	'''
	use websocket `ws` to download all desired calls in `calls` to `args.out_dir` with the following schema:

	$out_dir/system_id/talkgroup_id/call_file.m4a

	Additionally we need to save the job configuration and progress so we
	can resume if it gets stopped
	'''

	# for now, assume out_dir is fully empty and needs creating
	# collect all (system, talkgroup) tuples, rather than managing it on the fly
	out_dir = args.outdir
	sys_tg_tuples = { (c['system'], c['talkgroup']) for c in calls_meta }

	for tup in sys_tg_tuples:
		os.makedirs(os.path.join(out_dir, str(tup[0]), str(tup[1])), exist_ok=True)

	# save args as configuration, beginning progress
	# TODO: check if the progress file already exists to avoid clobbering a resumable download
	uri_domain = args.URI.split('/')[2]
	config_filename = os.path.join(out_dir, f"batch-download-{uri_domain}-{datetime.datetime.now().isoformat()}.config")
	progress_index_filename = os.path.join(out_dir, f"progress-index.txt") # contains index into call_metadata_list of current call to download, or "done"
	config = {"args": argparse_config_to_jsonable_obj(args), "call_metadata_list": calls_meta}

	# initial state
	with open(config_filename, 'w') as f:
		f.write( json.dumps(config) )
	with open(progress_index_filename, 'w') as f:
		f.write( str(0) )

	# download the calls, going in the same order as the call_meta list, saving our full list and progress
	# after each successful call downloads.
	for i in trange(len(calls_meta)):
		with open(progress_index_filename, 'w') as f:
			f.write( str(i) )

		call_meta = calls_meta[i]
		call_path = os.path.join(out_dir, str(call_meta['system']), str(call_meta['talkgroup']))

		#print(f"downloading call {call_meta}")
		result = download_one_call(ws, call_meta['id'])
		if result[0] == "CAL":
			call = result[1]
			save_one_call(call_meta, call, out_dir)
		else:
			print(f"error: unexpected response: {result}")
			quit()

		# TODO: update progress, both for user UI and for state file (probably try tqdm or alive-progress)

	with open(progress_index_filename, 'w') as f:
		f.write("done")

	return

def argparse_config_to_jsonable_obj(args):
	'''
	convert argparse args to json, since it isn't trivially serializable on its own.

	This means this function will have to keep up with the arguments we have for our
	program.
	'''
	return {
		"uri": args.URI,
		"begin": args.begin,
		"end": args.end,
		"talkgroups": args.talkgroups,
		"verbose": args.verbose
	}

def main():
	parser = argparse.ArgumentParser(
		description="bulk download calls from a rdio-scanner server",
		epilog="begin/end times MUST be in an ISO 8601 format. Times are assumed to be in the local time. For example, 8am on May 21st 2024 is 2024-05-21T08:00:00.000")

	parser.add_argument('URI',help='WS or WSS URI of rdio-scanner server to communicate with. example: wss://rdio-scanner.website.org/')
	parser.add_argument('--begin',help='begin time for timerange selection. ISO 8601 format in the UTC timezone.')
	parser.add_argument('--end',help='end time for timerange selection. ISO 8601 format in the UTC timezone.')
	parser.add_argument('outdir',help='directory to save calls under')
	parser.add_argument('--fetch-server-config-only',action='store_true',help='only fetch server configuration')
	parser.add_argument('--talkgroups', '--tgs', type=str, help="comma-separated list of talkgroup IDs")
	parser.add_argument('--verbose', '-v', action='store_true', help='verbose output')

	args = parser.parse_args()

	# check if out dir exists. If it does and has a config file, we should try resuming that download
	if os.path.exists(args.outdir):
		# check for a config file. If one exists, assume we're resuming a batch download
		config_glob_path = os.path.join(args.outdir, '*.config')
		progress_file_path = os.path.join(args.outdir, 'progress-index.txt')
		possible_configs = glob.glob(config_glob_path)
		if len(possible_configs) != 0:
			print(f"{possible_configs=}")
			if len(possible_configs) > 1:
				print(f"[WARNING] multiple configs found! Remove unwanted ones so there is only one config to resume.")
				quit()
			if os.path.isfile(progress_file_path):
				with open(progress_file_path, 'r') as f:
					index = f.read()
					if str(index) == "done":
						print(f"Not resuming a completed download")
			else:
				print(f"config found, but not progress-index.txt file found to resume. Just delete things and start over")
				quit()
			print(f"Previous config found in outdir, trying to resume batch download {possible_configs[0]}")
			resume_batch_download(args.outdir)
			quit()

	# normalize talkgroups option into array if present
	if args.talkgroups is not None:
		args.talkgroups = [int(s.strip()) for s in args.talkgroups.split(',')]

	if args.verbose:
		print(f"{args=}")

	# Accept only properly formatted begin/end times, normalize them
	# bring back to a string, so our other code doesn't have to think about datetimes anymore
	if args.begin:
		args.begin = datetime.datetime.fromisoformat(args.begin)
		prev_time = args.begin
		args.begin = args.begin.astimezone(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')
		if args.verbose:
			print(f"normalized begin time {prev_time} to {args.begin}")
	if args.end:
		args.end = datetime.datetime.fromisoformat(args.end)
		prev_time = args.end
		args.end = args.end.astimezone(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')
		if args.verbose:
			print(f"normalized end time {prev_time} to {args.end}")
	if args.begin and args.end:
		# TODO: check begin is before end
		#args.end - args.begin
		pass

	# fetch version & config via websocket
	server_version = None
	server_config = None
	with connect(args.URI) as ws:
		server_version, server_config = get_version_and_config(ws)

		if args.fetch_server_config_only:
			pretty_config = json.dumps(server_config[1]['systems'], indent=2)
			#print(pretty_config)
			filename = f"server-config-{args.URI.split('/')[2]}-{datetime.datetime.now().isoformat()}.json"
			os.makedirs(args.outdir) # make sure the directory exists
			config_file_path = os.path.join(args.outdir, filename)
			with open(config_file_path, "w") as f:
				f.write(pretty_config)
			print(f"config saved to {filename}")
			quit()

		# Have config, create talkgroup/system maps
		talkgroups, tg_id_to_label, tg_label_to_id = get_talkgroups_from_config(server_config)

		# sanity check selected talkgroups against filter
		try:
			selected_tg_labels = [tg_id_to_label[tgid] for tgid in args.talkgroups]
		except KeyError as e:
			print(f"unknown talkgroup id {e}")
			quit()

		print(f"{selected_tg_labels=}")

		# collect metainformation for all calls matching our filter
		calls_meta = []
		for tgid in args.talkgroups:
			res = fetch_call_meta_list(ws, talkgroups[tgid], args.begin, args.end)
			calls_meta.extend(res)

		print(f"found {len(calls_meta)} calls between {args.begin} and {args.end} for talkgroups {args.talkgroups}")

		# start downloading calls, new batch
		download_calls_new(ws, calls_meta, args)
	pass

if __name__ == "__main__":
	main()
