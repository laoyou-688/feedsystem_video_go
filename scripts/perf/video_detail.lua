wrk.method = "POST"
wrk.body   = '{"id":__VIDEO_ID__}'
wrk.headers["Content-Type"] = "application/json"

local cache_mode = os.getenv("CACHE_MODE")
if cache_mode ~= nil and cache_mode ~= "" then
  wrk.headers["X-Cache-Mode"] = cache_mode
end

request = function()
  return wrk.format(nil, nil, nil, wrk.body)
end
