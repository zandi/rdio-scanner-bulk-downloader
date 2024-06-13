# rdio-scanner bulk downloader

rdio-scanner is great, but one issue I ran across is the difficulty in
downloading multiple calls. Small numbers of calls can be manually downloaded,
but more than this and the manual process is too much work.

This bulk downloader uses the lazily reverse-engineered websockets API that
the javascript frontend uses to search for and download calls. Using it we
can search for all calls within a timerange and in certain talkgroups, and
download them.

# Requirements

Written using `Python 3.9.6`, and the `tqdm` and `websockets` packages.
see requirements.txt for specifics.

# Installing & Running

Create a virtual-env and install the required packages.

```
$ python3 -m venv my-venv
$ source my-venv/bin/activate
(my-venv) $ pip3 install --requirement requirements.txt
```

Usage is easy to get:

```
$ ./fetch_calls.py --help
usage: fetch_calls.py [-h] [--begin BEGIN] [--end END] [--fetch-server-config-only] [--talkgroups TALKGROUPS] [--verbose] URI outdir
...
```

A good first task is getting the server config. This is a useful reference for
what systems and talkgroups the rdio-scanner instance knows about.

```
$ ./fetch_calls.py --fetch-server-config-only wss://rdio-scanner.server.com out
config saved to server-config-rdio-scanner.server.com-2024-06-13T18:32:13.500721.json
```

For now, you can reference this config to know what talkgroups have which IDs. Now,
we can specify a timerange, talkgroups, and download our calls!

```
$ ./fetch_calls.py --talkgroups 12302 wss://rdio-scanner.server.com out --begin 2024-06-13T16:30:00 --end 2024-06-13T16:40:00
selected_tg_labels=['DISPATCH']
found 20 calls between 2024-06-13T20:30:00Z and 2024-06-13T20:40:00Z for talkgroups [12302]
100%|██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 20/20 [00:00<00:00, 21.32it/s]
```

Now our output directory has all saved calls in the structure `out/system_id/talkgroup_id/call_audio.file`.
