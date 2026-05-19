wrk.method = "POST"
wrk.body   = '{"limit":10}'
wrk.headers["Content-Type"] = "application/json"
wrk.headers["X-Feed-Session"] = "perf-session-001"

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
