// A tiny wrapper around fetch(), borrowed from
// https://kentcdodds.com/blog/replace-axios-with-a-simple-custom-fetch-wrapper

export function assemble_absolute_endpoint(relative_endpoint) {
  let url = new URL(window.location.href);
  let backend_port = process.env.REACT_APP_BACKEND_PORT;
  let backend_origin = backend_port ? url.origin.replace(url.port, backend_port) : url.origin;
  return `${backend_origin}${relative_endpoint}`
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
  try {
    const response = await window.fetch(endpoint, config)
    data = await response.json()
    if (response.ok) {
      return data
    }
    throw new Error(response.statusText)
  } catch (err) {
    return Promise.reject(err.message ? err.message : data)
  }
}

client.get = function (endpoint, customConfig = {}) {
  return client(endpoint, {...customConfig, method: 'GET'})
}

client.post = function (endpoint, body, customConfig = {}) {
  return client(endpoint, {...customConfig, body})
}
