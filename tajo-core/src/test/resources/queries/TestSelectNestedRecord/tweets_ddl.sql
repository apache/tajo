CREATE EXTERNAL TABLE IF NOT EXISTS ${0} (
  coordinates TEXT,
  favorited BOOL,
  truncated BOOL,
  created_at TIMESTAMP,
  id_str TEXT,
  /*entrities RECORD ( -- when we support array, we should remove this comment.
    urls ARRAY<TEXT>
  )*/
  in_reply_to_user_id_str TEXT,
  contributors TEXT,
  text TEXT,
  metadata RECORD (
    iso_language_code TEXT,
    result_type TEXT
  ),
  retweet_count INTEGER,
  in_reply_to_status_id_str TEXT,
  id TEXT,
  geo TEXT,
  retweeted BOOL,
  in_reply_to_user_id TEXT,
  place TEXT,
  user RECORD (
    profile_sidebar_fill_color TEXT,
    profile_sidebar_border_color TEXT,
    profile_background_tile TEXT,
    name TEXT,
    profile_image_url TEXT,
    created_at TIMESTAMP,
    location TEXT,
    follow_request_sent TEXT,
    profile_link_color TEXT,
    is_translator BOOL,
    id_str TEXT,
    /* -- when we support array, we should fill the following comments.
    entities RECORD (
      url RECORD (
      ),
      description RECORD (
      )
    ), */
    default_profile BOOL,
    contributors_enabled BOOL,
    favourites_count INTEGER,
    url TEXT,
    profile_image_url_https TEXT,
    utc_offset INTEGER,
    id BIGINT,
    profile_use_background_image BOOL,
    listed_count INTEGER,
    profile_text_color TEXT,
    lang TEXT,
    followers_count INTEGER,
    protected BOOL,
    notifications TEXT,
    profile_background_image_url_https TEXT,
    profile_background_color TEXT,
    verified TEXT,
    geo_enabled TEXT,
    time_zone TEXT,
    description TEXT,
    default_profile_image TEXT,
    profile_background_image_url TEXT,
    statuses_count INTEGER,
    friends_count INTEGER,
    following TEXT,
    show_all_inline_media BOOL,
    screen_name TEXT
  ),
  in_reply_to_screen_name TEXT,
  source TEXT,
  in_reply_to_status_id TEXT
) USING JSON LOCATION ${table.path};