// A tiny wrapper around fetch(), borrowed from
// https://kentcdodds.com/blog/replace-axios-with-a-simple-custom-fetch-wrapper

export function assemble_absolute_endpoint(relative_endpoint) {
  let backend_api = process.env.REACT_APP_BACKEND_API;
  let api_prefix = backend_api ? backend_api : (new URL(window.location.href)).origin;
  return `${api_prefix}${relative_endpoint}`
}

export async function client(endpoint, {body, ...customConfig} = {}) {
  const headers = {'Content-Type': 'application/json'}

  const config = {
    method: body ? 'POST' : 'GET',
    ...customConfig,
    headers: {
      ...headers,
      ...customConfig.headers,
    },
  }

  if (body) {
    config.body = JSON.stringify(body)
  }

  let data
  const response = await window.fetch(endpoint, config)
  data = await response.json()
  if (response.ok) {
    return data
  } else {
    // 4XX or 5XX, try use response.json() first, if not then fallback to statusText
    return Promise.reject(data ? data : response.statusText)
  }
}

client.get = function (endpoint, customConfig = {}) {
  return client(endpoint, {...customConfig, method: 'GET'})
}

client.post = function (endpoint, body, customConfig = {}) {
  return client(endpoint, {...customConfig, body})
}
