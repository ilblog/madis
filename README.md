## Synopsis

A downloader for NOAA's Meteorological Assimilation Data Ingest System (MADIS). MADIS consists of a live database of ground weather station observations from around the world.

For more information, and to sign up for a user account, visit: [https://madis-data.noaa.gov](https://madis-data.noaa.gov)

## Code Example

```javascript
var madis = require('madis')

var opts = {
  user: 'USERNAME',           // (REQUIRED) username
  pass: 'PASSWORD',           // (REQUIRED) password
  fields: ['T', 'PCPRATE'],   // (REQUIRED) Fields to query
  timeout: 120,               // Request timeout in seconds (Defaults to 120)
  qc: true,                   // Quality control enabled (Defaults to false)
  onlyLatest: true,           // Only return the latest observation per station (Defaults to false)
  window: 2*3600              // Search window in seconds (Defaults to two hours)
}

madis.download(opts, function(err, observations) {
  // observations contains the array of station observations
})
```
