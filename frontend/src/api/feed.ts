import { postJson } from './client'
import type { ListByFollowingResponse, ListByPopularityResponse, ListLatestResponse, ListLikesCountResponse } from './types'

const FEED_SESSION_KEY = 'feedsystem.feed.session'

function getFeedSessionId() {
  if (typeof window === 'undefined') return 'server-render'

  const cached = window.localStorage.getItem(FEED_SESSION_KEY)
  if (cached) return cached

  const session = `feed-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`
  window.localStorage.setItem(FEED_SESSION_KEY, session)
  return session
}

function feedOptions(authRequired = false) {
  return {
    authRequired,
    headers: {
      'X-Feed-Session': getFeedSessionId(),
    },
  }
}

export function listLatest(input: { limit: number; latest_time: number; latest_id_before?: number; sort_mode?: 'latest' | 'hybrid' }) {
  return postJson<ListLatestResponse>('/feed/listLatest', input, feedOptions())
}

export function listLikesCount(input: { limit: number; likes_count_before?: number; id_before?: number }) {
  const body: Record<string, unknown> = { limit: input.limit }
  if (typeof input.likes_count_before === 'number' || typeof input.id_before === 'number') {
    body.likes_count_before = input.likes_count_before ?? 0
    body.id_before = input.id_before ?? 0
  }
  return postJson<ListLikesCountResponse>('/feed/listLikesCount', body, feedOptions())
}

export function listByPopularity(input: { limit: number; as_of: number; offset: number }) {
  return postJson<ListByPopularityResponse>('/feed/listByPopularity', input, feedOptions())
}

export function listByFollowing(input: { limit: number; latest_time: number; latest_id_before?: number }) {
  return postJson<ListByFollowingResponse>('/feed/listByFollowing', input, feedOptions(true))
}
