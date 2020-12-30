

BUCKET=bucket-e123

PROJECT=haoran-assignment2

python3 assignment2.py --input gs://\$BUCKET/Assignment2/video_games.csv --output gs://\$BUCKET/Assignment2/output/game_numbers.csv --runner DataflowRunner --project \$PROJECT --temp_location gs://$BUCKET/tmp/ --game_numbers --num_workers 4

python3 assignment2.py --input gs://\$BUCKET/Assignment2/video_games.csv --output gs://\$BUCKET/Assignment2/output/game_revenue.csv --runner DataflowRunner --project \$PROJECT --temp_location gs://$BUCKET/tmp/ --game_revenue --num_workers 4

python3 assignment2.py --input gs://\$BUCKET/Assignment2/video_games.csv --output gs://\$BUCKET/Assignment2/output/developer_numbers.csv --runner DataflowRunner --project \$PROJECT --temp_location gs://$BUCKET/tmp/ --developer_numbers --num_workers 4

python3 assignment2.py --input gs://\$BUCKET/Assignment2/video_games.csv --output gs://\$BUCKET/Assignment2/output/developer_revenue.csv --runner DataflowRunner --project \$PROJECT --temp_location gs://$BUCKET/tmp/ --developer_revenue --num_workers 4

python3 assignment2.py --input gs://\$BUCKET/Assignment2/video_games.csv --output gs://\$BUCKET/Assignment2/output/sports_numbers.csv --runner DataflowRunner --project \$PROJECT --temp_location gs://$BUCKET/tmp/ --game_numbers --genre Sports --num_workers 4

python3 assignment2.py --input gs://\$BUCKET/Assignment2/video_games.csv --output gs://\$BUCKET/Assignment2/output/sports_revenue.csv --runner DataflowRunner --project \$PROJECT --temp_location gs://$BUCKET/tmp/ --game_revenue --genre Sports --num_workers 4

python3 assignment2.py --input gs://\$BUCKET/Assignment2/video_games.csv --output gs://\$BUCKET/Assignment2/output/sports_developer_numbers.csv --runner DataflowRunner --project \$PROJECT --temp_location gs://$BUCKET/tmp/ --developer_numbers --genre Sports --num_workers 4

python3 assignment2.py --input gs://\$BUCKET/Assignment2/video_games.csv --output gs://\$BUCKET/Assignment2/output/sports_developer_revenue.csv --runner DataflowRunner --project \$PROJECT --temp_location gs://$BUCKET/tmp/ --developer_revenue --genre Sports --num_workers 4

python3 assignment2.py --input gs://\$BUCKET/Assignment2/video_games.csv --output gs://\$BUCKET/Assignment2/output/most_purchased_together.csv --runner DataflowRunner --project \$PROJECT --temp_location gs://$BUCKET/tmp/ --purchased_together --num_workers 4



Shards:

After creating outputs on cloud, use compose method to cancatenate several shards together. For example, for  most_purchased_together(note: I am using zsh for commands, so I added "" on the URI. It will not be necessary in usual command environment):

gsutil compose \
  "gs://\${BUCKET}/Assignment2/output/most_purchased_together.csv*" \
  "gs://​\${BUCKET}/Assignment2/output/most_purchased_together.csv"