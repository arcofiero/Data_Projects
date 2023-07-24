# To copy all JSON Reference data to same location:
aws s3 cp . s3://dae-youtube-raw-useast1-dev/youtube/raw_statistics_reference_data/ --recursive --exclude "*" --include "*.json"

# To copy all data files to its own location, following Hive-style patterns:
aws s3 cp CA_youtube_trending_data.csv s3://dae-youtube-raw-useast1-dev/youtube/raw_statistics/region=ca/
aws s3 cp DE_youtube_trending_data.csv s3://dae-youtube-raw-useast1-dev/youtube/raw_statistics/region=de/
aws s3 cp FR_youtube_trending_data.csv s3://dae-youtube-raw-useast1-dev/youtube/raw_statistics/region=fr/
aws s3 cp GB_youtube_trending_data.csv s3://dae-youtube-raw-useast1-dev/youtube/raw_statistics/region=gb/
aws s3 cp IN_youtube_trending_data.csv s3://dae-youtube-raw-useast1-dev/youtube/raw_statistics/region=in/
aws s3 cp JP_youtube_trending_data.csv s3://dae-youtube-raw-useast1-dev/youtube/raw_statistics/region=jp/
aws s3 cp KR_youtube_trending_data.csv s3://dae-youtube-raw-useast1-dev/youtube/raw_statistics/region=kr/
aws s3 cp MX_youtube_trending_data.csv s3://dae-youtube-raw-useast1-dev/youtube/raw_statistics/region=mx/
aws s3 cp RU_youtube_trending_data.csv s3://dae-youtube-raw-useast1-dev/youtube/raw_statistics/region=ru/
aws s3 cp US_youtube_trending_data.csv s3://dae-youtube-raw-useast1-dev/youtube/raw_statistics/region=us/
