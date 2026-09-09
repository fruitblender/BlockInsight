const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000').replace(/\/$/, '')

export class ApiError extends Error {
  constructor(message, status = 0) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

async function request(path, options = {}) {
  let response
  try {
    response = await fetch(`${API_BASE_URL}${path}`, {
      ...options,
      headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
    })
  } catch {
    throw new ApiError('Unable to reach the BlockInsight API. Check that FastAPI is running.')
  }

  const raw = await response.text()
  let payload = null
  if (raw) {
    try {
      payload = JSON.parse(raw)
    } catch {
      throw new ApiError('The API returned an invalid JSON response.', response.status)
    }
  }

  if (!response.ok) {
    const detail = payload?.detail || `Request failed with status ${response.status}.`
    throw new ApiError(detail, response.status)
  }
  return payload
}

export const api = {
  baseUrl: API_BASE_URL,
  get: (path) => request(path),
  post: (path, body) => request(path, { method: 'POST', body: JSON.stringify(body) }),
}

export function formatApiError(error) {
  return error instanceof ApiError ? error.message : 'Something went wrong while loading this data.'
}
