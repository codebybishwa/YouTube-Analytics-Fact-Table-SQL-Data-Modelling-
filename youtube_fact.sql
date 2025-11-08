create or replace table fact.youtube_fact as 
with base as
  (select * from youtube.src_youtube_channel_list)

,channel_metadata as 
  (select * 
    ,row_number() over(partition by channelId order by channelViewCount desc) dataRank
  from dimension.dim_src_all_youtube_channel_metadata
  qualify dataRank=1)

,youtube_videos as 
  (select * from `dimension.dim_src_all_youtube_videos`)

,youtube_video_metadata as 
  (select * from dimension.dim_src_all_youtube_videos_metadata)

,youtube_video_stats as 
  (select * from dimension.dim_src_youtube_video_stats)

,final as
  (select base.channelId
    ,channel_metadata.channelTitle
      ,youtube_videos.videoId
    ,youtube_videos.title as videoTitle
    ,youtube_videos.publishedAt as videoPublishedAt
    ,cast(youtube_video_metadata.videoDuration as float64) videoDuration
    ,cast(youtube_video_metadata.videoDuration as float64)/60 videoDurationMin
    ,cast(youtube_video_stats.videoViews as int) videoViews
    ,cast(youtube_video_stats.videoLikes as int) videoLikes
    ,cast(youtube_video_stats.videoComments as int)  videoComments
    
    ,youtube_video_metadata.videoQuality
    ,youtube_videos.description as videoDescription
    ,channel_metadata.channelDescription
    ,cast(channel_metadata.ChannelsubscriberCount as int) ChannelsubscriberCount
    ,cast(channel_metadata.channelVideoCount as int) channelVideoCount
    ,cast(channel_metadata.channelViewCount as int)  channelViewCount
    ,channel_metadata.channelPublishedAt
    ,base.url

  from base
  left join channel_metadata on channel_metadata.channelId=base.channelId
  left join youtube_videos on youtube_videos.channelId=base.channelId
  left join youtube_video_metadata on youtube_video_metadata.videoId=youtube_videos.videoId
  left join youtube_video_stats on youtube_video_stats.videoId=youtube_videos.videoId)

select * 
  ,case WHEN videoDuration is null THEN '0.NA'
    when videoDuration <60 then '1. Short (<1 min)'
      WHEN videoDuration < 300 THEN '2. Medium (1–5 min)'
    WHEN videoDuration < 900 THEN '3. Long (5–15 min)'
    WHEN videoDuration < 1800 THEN '4. Extended (15–30 min)'
    ELSE '5. Very Long (30+ min)'
  END AS videoDurationBucket
  ,CASE
    WHEN videoViews IS NULL THEN '0.NA'
    WHEN videoViews < 50000 THEN '1. Very Low (<50K)'
    WHEN videoViews < 250000 THEN '2. Low (50K-250K)'
    WHEN videoViews < 750000 THEN '3. Medium (250K-750K)'
    WHEN videoViews < 2000000 THEN '4. High (750K-2M)'
    ELSE '5. Viral (2M+)'
END AS videoViewsBucket

from final