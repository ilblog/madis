// http://madis.noaa.gov/doc/sfc_mesonet_variable_list.txt

var request = require("request")

var FIELDS = {
  'TD': {qc:true},
  'TD1H': {qc:false},
  'TD15M': {qc:true},
  'TD1HCHG': {qc:false},
  'RH': {qc:true},
  'RH1H': {qc:false},
  'RH15M': {qc:true},
  'RH1HCHG': {qc:false},
  'Q': {qc:true},
  'Q1H': {qc:false},
  'Q15M': {qc:true},
  'Q1HCHG': {qc:false},
  'DPD': {qc:true},
  'DPD1H': {qc:false},
  'DPD15M': {qc:true},
  'DPD1HC': {qc:false},
  'AH': {qc:true},
  'AH1H': {qc:false},
  'AH15M': {qc:true},
  'AH1HCHG': {qc:false},
  'WVMR': {qc:true},
  'WVMR15M': {qc:true},
  'WVMR1HC': {qc:false},
  'ALTSE': {qc:true},
  'ALTS15M': {qc:true},
  'AL1HCHG': {qc:false},
  'PT3': {qc:false},
  'SLP': {qc:true},
  'P': {qc:true},
  'P15M': {qc:true},
  'T': {qc:true},
  'T1H': {qc:false},
  'T15M': {qc:true},
  'T1HCHG': {qc:false},
  'ARCHVT': {qc:true},
  'TV': {qc:true},
  'TV1H': {qc:false},
  'TV15M': {qc:true},
  'TV1HCHG': {qc:false},
  'DD': {qc:true},
  'DD1H': {qc:false},
  'DD15M': {qc:false},
  'DDM15M': {qc:false},
  'DDU15M': {qc:false},
  'DD24H': {qc:false},
  'DDSD15M': {qc:false},
  'FF': {qc:true},
  'FF1H': {qc:false},
  'FF15M': {qc:true},
  'FF24H': {qc:false},
  'FFSD15M': {qc:true},
  'U': {qc:true},
  'U1H': {qc:false},
  'U15M': {qc:true},
  'U24H': {qc:false},
  'USD15M': {qc:true},
  'V': {qc:true},
  'V1H': {qc:false},
  'V15M': {qc:true},
  'V24H': {qc:false},
  'VSD15M': {qc:true},
  'VIS': {qc:false},
  'PCP5M': {qc:false, avg:true},
  'PCP15M': {qc:false, avg:true},
  'PCP1H': {qc:false},
  'PCP3H': {qc:false},
  'PCP6H': {qc:false},
  'PCP12H': {qc:false},
  'PCP18H': {qc:false},
  'PCP24H': {qc:false},
  'PCPLM': {qc:false},
  'PCPUTCM': {qc:false},
  'PCPCDAY': {qc:false},
  'PCPUDAY': {qc:false},
  'PCPRATE': {qc:false, avg:true},
  'ARCHPCP': {qc:false},
  'SOILMP': {qc:false},
  'SOILM2': {qc:false},
  'SOILM4': {qc:false},
  'SOILM8': {qc:false},
  'SOILM20': {qc:false},
  'SOILM40': {qc:false},
  'SOILT': {qc:true},
  'SOILT2': {qc:true},
  'SOILT4': {qc:true},
  'SOILT8': {qc:true},
  'SOILT20': {qc:true},
  'SOILT40': {qc:true},
  'DDGUST': {qc:false},
  'FFGUST': {qc:false},
  'FF24MAX': {qc:false},
  'T24MIN': {qc:false},
  'T24MAX': {qc:false},
  'DDMAX1H': {qc:false},
  'FFMAX1H': {qc:true},
  'SNOWC': {qc:false},
  'SNOW6H': {qc:false},
  'SNOW24H': {qc:false},
  'SST': {qc:true},
  'ELEV': {qc:false},
  'SKYCVLB': {qc:false, numCols:6},
  'SKYCOV': {qc:false, type:'text', numCols:6},
  'PCPTYPE': {qc:false, numCols:2},
  'PRESWEA': {qc:false, type:'text'}
}

function lerp(a, b, x) {
  // NOTE: we don't cap x to 0-1 because we want to use this for extrapolation as well -ARG
  return a + (b - a) * x
}

function lerpObservations(a, b, x) {
  var c = {
    station_name: a.station_name,
    lat: a.lat,
    lon: a.lon,
    elev: a.elev
  }
  var f
  
  for(var field in a) {
    f = FIELDS[field]
    if(!f) {
      continue
    }
    
    if(a[field] == null && b[field] == null) {
      continue
    }
    
    if(a[field] == null) {
      c[field] = b[field]
      
    } else if(b[field] == null) {
      c[field] = a[field]
      
    } else if(field == 'PCPTYPE') {
      c[field] = avgPCPTYPE([a['PCPTYPE'], b['PCPTYPE']])
      
    } else if(field == 'SKYCOV') {
      c[field] = lerpSKYCOV(a['SKYCOV'], b['SKYCOV'], x)
    
    } else if(field == 'PRESWEA') {
      var vals = []
      if(a[field]) {
        vals = vals.concat(a[field].split(' '))
      }
      if(b[field]) {
        vals = vals.concat(b[field].split(' '))
      }
      c[field] = (vals.length == 0) ? null : vals
    
    } else if(f.numCols > 1) {
      c[field] = []
      for(var i = 0; i < f.numCols; i++) {
        c[field][i] = lerp(a[field][i], b[field][i], x)
      }
    
    } else if(f.type == 'text') {
      var vals = []
      if(a[field]) vals.push(a[field])
      if(b[field]) vals.push(b[field])
      c[field] = (vals.length == 0) ? null : vals
    
    } else {
      c[field] = lerp(a[field], b[field], x)
    }
  }
  
  return c
}

/*
Value      Meaning
-----      -------
 0        no precipitation
 1        precipitation present but unclassified
 2        rain
 3        snow
 4        mixed rain and snow
 5        light
 6        light freezing
 7        freezing rain
 8        sleet
 9        hail
10        other
11        unidentified
12        unknown
13        frozen
14        ice pellets
15        recent
29        RPU-to-maxSensor communications failure
30        sensor failure
*/
var PCPTYPE_FAIL_VALS = [29, 30]
var PCPTYPE_UNKNOWN_VALS = [1, 5, 10, 11, 12, 15]
var PCPTYPE_FREEZE_VALS = [6, 7, 8, 13, 14]

function avgPCPTYPE(vals) {
  
  // Remove any WTF values
  for(var i = vals.length-1; i >= 0; i--) {
    if(!vals[i] || vals[i][0] == null || isNaN(vals[i][0]) || PCPTYPE_FAIL_VALS.indexOf(vals[i][0]) != -1) {
      vals.splice(i, 1)
    }
  }
  
  if(vals.length == 0)
    return null
  
  var b = vals[0][1]
  
  var has_none = false,
      has_rain = false,
      has_snow = false,
      has_freeze = false,
      has_hail = false,
      has_unknown = false,
      val
  
  for(var i = 0; i < vals.length; i++) {
    val = vals[i][0]
    
    if(val == 0) {
      has_none = true
      
    } else if(val == 2) {
      has_rain = true

    } else if(val == 3) {
      has_snow = true

    } else if(val == 4) {
      has_rain = has_snow = true
    
    } else if(val == 9) {
      has_hail = true
      
    } else if(PCPTYPE_FREEZE_VALS.indexOf(val) != -1) {
      has_freeze = true
      
    } else if(PCPTYPE_UNKNOWN_VALS.indexOf(val) != -1) {
      has_unknown = true
    }
  }
  
  // Return the most relevant result
  if(has_hail)
    return [9, b]
  
  if(has_rain && has_snow)
    return [4, b]
  
  if(has_freeze)
    return [8, b]
  
  if(has_rain)
    return [2, b]
      
  if(has_snow)
    return [3, b]
  
  if(has_none)
    return [0, b]
  
  // Default to unknown
  return [12, b]
}

function skyToPercent(cov) {
  switch(cov) {
    case "CLR": case "SKC": return 0.0;
    case "FEW": return 0.1875;
    case "SCT": return 0.4375;
    case "BKN": return 0.75;
    case "OVC": case "VV": return 1.0;
    default: return NaN;
  }
}

function percentToSky(percent) {
  if(percent < 1.0/32.0) {
    return "CLR"
  } else if(percent < 2.0/8.0) {
    return "FEW"
  } else if(percent < 4.0/8.0) {
    return "SCT"
  } else if(percent < 7.0/8.0) {
    return "BKN"
  } else {
    return "OVC"
  }
}

function lerpSKYCOV(a, b, x) {
  var c = []
  var numCols = FIELDS['SKYCOV'].numCols
  
  for(var i = 0; i < numCols; i++) {

    if(a[i] == b[i]) {
      c.push(a[i])
      continue
    }
  
    var a_val = skyToPercent(a[i])
    var b_val = skyToPercent(b[i])
  
    if(isNaN(a_val)) {
      c.push(b[i])
    } else if(isNaN(b_val)) {
      c.push(a[i])
    } else {
      c.push(percentToSky(lerp(a_val, b_val, x)))
    }
  }
  
  return c
}

function avgSKYCOV(vals) {
  var c = []
  var numCols = FIELDS['SKYCOV'].numCols
  
  for(var i = 0; i < numCols; i++) {
    var val = 0,
        n = 0,
        p
    
    for(var j = 0; j < vals.length; j++) {
      p = skyToPercent(vals[j][i])
      if(isNaN(p)) continue
      val += p
      n++
    }
    
    c.push(n == 0 ? '' : percentToSky(val / n))
  }
  
  return c
}

function query(opts, callback) {
  
  var qcsel = opts.qc ? 3 : 1,
      minbck = opts.minbck ? Math.round(opts.minbck/-60) :
               opts.window ? Math.round(opts.window/-60) : -120,
      minfwd = opts.minfwd ? Math.round(opts.minfwd/60) : 0,
      recwin = opts.onlyLatest ? 1 : 4,
      time = opts.time
  
  if(!time) {
    time = '0'
  } else {
    var date = new Date(1000*time)
    time = date.getUTCFullYear()+("0"+(1+date.getUTCMonth())).slice(-2)+("0"+date.getUTCDate()).slice(-2)+"_"+("0"+date.getUTCHours()).slice(-2)+"00"
  }
  
  var url = 'https://madis-data.ncep.noaa.gov/madisPublic1/cgi-bin/madisXmlPublicDir?rdr=&time='+time+'&minbck='+minbck+'&minfwd='+minfwd+'&recwin='+recwin+'&dfltrsel=0&latll=0.0&lonll=0.0&latur=90.0&lonur=0.0&stanam=&stasel=0&pvdrsel=0&varsel=1&qcsel='+qcsel+'&xml=2&csvmiss=1&nvars=LAT&nvars=LON&nvars=ELEV'
  
  for(var i = 0; i < opts.fields.length; i++) {
    url += '&nvars='+opts.fields[i]
  }
  
  var auth = null
  if(opts.user && opts.pass) {
    auth = {
      user: opts.user,
      pass: opts.pass
    }
  }
  
  return request({
      uri: url,
      auth: auth,
      timeout: opts.timeout ? 1000*opts.timeout : 120000
    },
    function(err, res, data) {
      if(err) {
        return callback(err, opts, null)
    
      } else if(res.statusCode !== 200) {
        return callback(new Error("Received a non-200 response from madis-data.noaa.gov."), opts, null)
    
      } else {
        parseObservationString(opts, data, function(err, observations) {
          if(err)
            return callback(err, opts, null)
          
          return callback(null, opts, observations)
        })
      }
    }
  )
}

function parseObservationString(opts, str, callback) {
  var a = str.indexOf("<PRE>"),
      b = str.indexOf("</PRE>", a),
      fields = opts.fields,
      observations = {},
      i, cols
  
  if(a == -1 || b == -1) {
    return callback(new Error('Invalid format'))
  }
  
  var lines = str.slice(a + 5, b).split(/\n/g)
  
  for(i = 0; i !== lines.length; i++) {
    cols = lines[i].split(/,/g)
    
    if(cols.length < 8) {
      continue
    }
    
    var obs = {
      station_name: cols[0].trim(),
      lat: parseFloat(cols[5]),
      lon: parseFloat(cols[6]),
      elev: parseFloat(cols[7]),
      timestamp: unix(cols[1], cols[2])
    },
    id = obs.station_name+'-'+obs.timestamp
    
    var col_i = 8,
        n_cols = 1,
        field
    
    for(var f = 0; f < fields.length; f++) {
      field = fields[f]
      n_cols = FIELDS[field].numCols || 1
      
      if(n_cols == 1) {
        obs[field] = (FIELDS[field].type == 'text') ?
                     cols[col_i].trim() :
                     parseFloat(cols[col_i])
        
      } else {
        obs[field] = []
        for(var c = 0; c < n_cols; c++) {
          obs[field].push((FIELDS[field].type == 'text') ?
                          cols[col_i+c].trim() :
                          parseFloat(cols[col_i+c]))
        }
      }
      
      col_i += n_cols
    }
    
    observations[id] = obs
  }
  
  return callback(null, observations)
}

function unix(date, time) {
  date = date.split("/");
  time = time.split(":");
  return (Date.parse(date[2] + "-" + date[0] + "-" + date[1] + "T" + time[0] + ":" + time[1] + ":00Z")/1000)|0;
}

function averageObservations(observations) {
  
  var obs = {
    station_name: observations[0].station_name,
    lat: observations[0].lat,
    lon: observations[0].lon,
    elev: observations[0].elev
  },
  f, val
  
  for(var field in observations[0]) {
    f = FIELDS[field]
    if(!f) {
      continue
    }
    
    val = null
    n = 0
    
    if(field == 'PCPTYPE') {
      var pcptypes = []
      for(var i = 0; i < observations.length; i++) {
        pcptypes.push(observations[i]['PCPTYPE'])
      }
      val = avgPCPTYPE(pcptypes)
      
    } else if(field == 'SKYCOV') {
      var skycovs = []
      for(var i = 0; i < observations.length; i++) {
        skycovs.push(observations[i]['SKYCOV'])
      }
      val = avgSKYCOV(skycovs)
    
    } else if(field == 'PRESWEA') {
      val = []
      for(var i = 0; i < observations.length; i++) {
        if(observations[i][field]) {
          val = val.concat(observations[i][field].split(' '))
        }
      }
    
    // Average multi-column values
    } else if(f.numCols > 1) {
      val = []
      var v
      
      for(var j = 0; j < f.numCols; j++) {
        v = 0
        n = 0
        
        for(var i = 0; i < observations.length; i++) {
          if(!isNaN(observations[i][field][j]) && observations[i][field][j] != null) {
            v += observations[i][field][j]
            n++
          }
        }
        
        val.push(n > 0 ? val / n : null)
      }
      
    // Average the values
    } else if(f.avg) {
      val = 0
      for(var i = 0; i < observations.length; i++) {
        if(!isNaN(observations[i][field]) && observations[i][field] != null) {
          val += observations[i][field]
          n++
        }
      }
      val = n > 0 ? val / n : null
    
    // If it's a text field, just create an array of values
    } else if(f.type == 'text') {
      val = []
      for(var i = 0; i < observations.length; i++) {
        if(observations[i][field]) {
          if(observations[i][field].concat) {
            val = val.concat(observations[i][field])
          } else {
            val.push(observations[i][field])
          }
        }
      }
      
      if(val.length == 0) {
        val = null
      }
    
    // Find the latest value as the default
    } else {
      val = null
      for(var i = observations.length-1; i >= 0; i--) {
        if(!isNaN(observations[i][field]) && observations[i][field] != null) {
          val = observations[i][field]
          break
        }
      }
    }
    
    obs[field] = val
  }
  
  return obs
}

// Interpolate or extrapolate to the given time, and add it to the observations
function addPointAtTimes(times, observations) {
  if(observations.length < 2)
    return observations
  
  var observations_sorted,
      time, obs, obs0, obs1,
      obs_to_add = []
  
  for(var t = 0; t < times.length; t++) {
    time = times[t]
    
    observations_sorted = observations.filter(function(a) {
      return Math.abs(a.timestamp - time) <= 3600
    }).sort(function(a, b) {
      return Math.abs(a.timestamp - time) - Math.abs(b.timestamp - time)
    })
    
    if(observations_sorted.length < 2)
      continue
    
    if(observations_sorted[1].timestamp > observations_sorted[0].timestamp) {
      obs0 = observations_sorted[0]
      obs1 = observations_sorted[1]
    } else {
      obs0 = observations_sorted[1]
      obs1 = observations_sorted[0]
    }
    
    var percent = (time - obs0.timestamp) / (obs1.timestamp - obs0.timestamp)
    obs = lerpObservations(obs0, obs1, percent)
    obs.timestamp = time
    obs_to_add.push(obs)
  }
  
  if(obs_to_add.length > 0) {
    observations = observations.concat(obs_to_add)
    observations.sort(function(a, b) {return a.timestamp - b.timestamp})
  }
  
  return observations
}

function compileHourly(time, observations, opts) {
  if(observations.length == 0) {
    return null
  }
  
  var time_start_hour = time - 3600
  
  // Add points at the start, middle, and end of the hour
  // (This ends up fixing a lot of edge cases)
  addPointAtTimes([time, Math.round((time_start_hour + time) / 2), time_start_hour], observations)
  
  // Remove any points outside of the hour
  observations = observations.filter(function(a) {
    return a.timestamp >= time_start_hour && a.timestamp <= time
  })
  
  // If there's nothing left, return null
  if(observations.length == 0) {
    return null
  }
  
  // Average all the remaining points
  var obs = averageObservations(observations)
  obs.timestamp = time
  
  return obs
}

function hasData(obs) {
  if(!obs)
    return false
  
  for(var field in obs) {
    if(!FIELDS[field])
      continue
    
    if(obs[field] !== null && obs[field] !== undefined)
      return true
  }
  
  return false
}

exports.download = function(opts, callback) {
  
  // Sanity check opts params
  if(!opts.fields || opts.fields.length == 0) {
    return callback(new Error('No fields specified.'))
  }
  
  for(var i = 0; i < opts.fields.length; i++) {
    if(!FIELDS[opts.fields[i]]) {
      return callback(new Error('Invalid field: '+opts.fields[i]+'. If it is valid, please add it to the FIELDS array.'))
    }
  }
  
  // Query the NOAA server
  var todo = 1,
      observations, observations_qc,
      req, req_qc
  
  var done = function(err, source_opts, obs) {
    
    if(err) {
      if(source_opts.qc) {
        if(req)
          req.abort()
      } else {
        if(req_qc)
          req_qc.abort()
      }
      
      return callback(err)
    }
    
    if(source_opts.qc) {
      observations_qc = obs
    } else {
      observations = obs
    }
    
    todo--
    
    if(todo === 0) {
      
      // Merge QC and non-QC data, if necessary
      if(observations && observations_qc) {
        for(var s in observations) {
          for(var f in observations[s]) {
            if(FIELDS[f] && FIELDS[f].qc && observations_qc[s] && observations_qc[s][f] != null && !isNaN(observations_qc[s][f])) {
              observations[s][f] = observations_qc[s][f]
            }
          }
        }
      
      } else if(observations_qc) {
        observations = observations_qc
      }
      
      // Convert to array
      var obs_array = []
      for(var s in observations) {
        obs_array.push(observations[s])
      }
      obs_array.sort(function(a, b) {return b.timestamp - a.timestamp})
      
      // Apply the blacklist
      if(opts.blacklist) {
        
        var blacklist = {}
        for(var i = 0; i < opts.blacklist.length; i++) {
          blacklist[opts.blacklist[i]] = true
        }
        
        for(var i = obs_array.length; i--; ) {
          if(blacklist[obs_array[i].station_name]) obs_array.splice(i, 1)
        }
      }
      
      return callback(null, obs_array)
    }
  }
  
  // If QC is on, we need to query twice to fill in missing data,
  // because MADIS is stupid (or I am).
  if(opts.qc) {
    
    // Do any of the fields actually not exist in QC results?
    var query_for_non_qc = false
    for(var i = 0; i < opts.fields.length; i++) {
      if(!FIELDS[opts.fields[i]].qc) {
        query_for_non_qc = true
        break
      }
    }
    
    if(query_for_non_qc) {
      todo++
      
      // FIXME: this is an ugly way to clone an object
      var opts2 = JSON.parse(JSON.stringify(opts))
      opts2.qc = false
      req_qc = query(opts2, done)
    }
  }
  
  req = query(opts, done)
}

exports.downloadHourly = function(time, opts, callback) {
  opts = opts || {}
  
  opts.time = time
  opts.minbck = 7200
  opts.minfwd = 3600
  opts.onlyLatest = false
  
  // Download the raw data
  exports.download(opts, function(err, observations) {
    if(err)
      return callback(err)
    
    // Collect all station observations and organize by station_name
    var stations = {},
        obs
    
    for(var i = 0; i < observations.length; i++) {
      obs = observations[i]
      
      if(!stations[obs.station_name]) {
        stations[obs.station_name] = []
      }
      
      stations[obs.station_name].push(obs)
    }
    
    observations = []
    
    // Sort each station's data by time, just in case
    for(var station_name in stations) {
      stations[station_name].sort(function(a, b){return a.timestamp - b.timestamp})
    }
    
    // Turn station data into a single hourly point
    for(var station_name in stations) {
      var obs = compileHourly(time, stations[station_name], opts)
      if(obs && hasData(obs)) {
        observations.push(obs)
      }
    }
    
    // Cast all missing data to NaN
    var obs
    for(var i = 0; i < observations.length; i++) {
      obs = observations[i]
      for(var field in obs) {
        if(obs[field] === null || obs[field] === undefined) {
          obs[field] = NaN
        }
      }
    }
    
    return callback(null, observations)
  })
}
