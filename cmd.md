Run CV: `guvrun -m dumper.cli run --vehicle-type cv`
Check Count: `guvrun -m dumper.cli count --vehicle-type cv`
Pause: `guvrun -m dumper.cli stop --vehicle-type cv`
Resume: `guvrun -m dumper.cli resume --vehicle-type cv`


S3_ENABLED=true `guvrun media_rapid_to_s3.py`

Handy variants:

`guvrun media_rapid_to_s3.py --target articles`       # one table only
`guvrun media_rapid_to_s3.py --limit 5000`            # cap per target
`guvrun media_rapid_to_s3.py --concurrency 48`        # more in flight
`guvrun media_rapid_to_s3.py --retry-failed`          # re-try the '' sentinels

guvrun scripts/sync_search_index.py

guvrun backfill_category_links.py