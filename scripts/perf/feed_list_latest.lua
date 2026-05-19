wrk.method = "POST"
local limit = os.getenv("LIMIT")
if limit == nil or limit == "" then
  limit = "10"
end
wrk.body   = string.format('{"limit":%s}', limit)
wrk.headers["Content-Type"] = "application/json"

local feed_session = os.getenv("FEED_SESSION")
if feed_session ~= nil and feed_session ~= "" then
  wrk.headers["X-Feed-Session"] = feed_session
end

local source_mode = os.getenv("FEED_SOURCE_MODE")
if source_mode ~= nil and source_mode ~= "" then
  wrk.headers["X-Feed-Source-Mode"] = source_mode
end

local entity_mode = os.getenv("FEED_ENTITY_CACHE_MODE")
if entity_mode ~= nil and entity_mode ~= "" then
  wrk.headers["X-Feed-Entity-Cache-Mode"] = entity_mode
end

request = function()
  return wrk.format(nil, nil, nil, wrk.body)
end
