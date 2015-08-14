var request = require("request")

// 1 - doesn't appear in QC results, 2 - appears in QC results
var FIELDS = {
  'TD':2,
  'TD1H':1,
  'TD15M':2,
  'TD1HCHG':1,
  'RH':2,
  'RH1H':1,
  'RH15M':2,
  'RH1HCHG':1,
  'Q':2,
  'Q1H':1,
  'Q15M':2,
  'Q1HCHG':1,
  'DPD':2,
  'DPD1H':1,
  'DPD15M':2,
  'DPD1HC':1,
  'AH':2,
  'AH1H':1,
  'AH15M':2,
  'AH1HCHG':1,
  'WVMR':2,
  'WVMR15M':2,
  'WVMR1HC':1,
  'ALTSE':2,
  'ALTS15M':2,
  'AL1HCHG':1,
  'PT3':1,
  'SLP':2,
  'P':2,
  'P15M':2,
  'T':2,
  'T1H':1,
  'T15M':2,
  'T1HCHG':1,
  'ARCHVT':2,
  'TV':2,
  'TV1H':1,
  'TV15M':2,
  'TV1HCHG':1,
  'DD':2,
  'DD1H':1,
  'DD15M':1,
  'DDM15M':1,
  'DDU15M':1,
  'DD24H':1,
  'DDSD15M':1,
  'FF':2,
  'FF1H':1,
  'FF15M':2,
  'FF24H':1,
  'FFSD15M':2,
  'U':2,
  'U1H':1,
  'U15M':2,
  'U24H':1,
  'USD15M':2,
  'V':2,
  'V1H':1,
  'V15M':2,
  'V24H':1,
  'VSD15M':2,
  'VIS':1,
  'PCP5M':1,
  'PCP15M':1,
  'PCP1H':1,
  'PCP3H':1,
  'PCP6H':1,
  'PCP12H':1,
  'PCP18H':1,
  'PCP24H':1,
  'PCPLM':1,
  'PCPUTCM':1,
  'PCPCDAY':1,
  'PCPUDAY':1,
  'PCPRATE':1,
  'ARCHPCP':1,
  'SOILMP':1,
  'SOILM2':1,
  'SOILM4':1,
  'SOILM8':1,
  'SOILM20':1,
  'SOILM40':1,
  'SOILT':2,
  'SOILT2':2,
  'SOILT4':2,
  'SOILT8':2,
  'SOILT20':2,
  'SOILT40':2,
  'DDGUST':1,
  'FFGUST':1,
  'FF24MAX':1,
  'T24MIN':1,
  'T24MAX':1,
  'DDMAX1H':1,
  'FFMAX1H':2,
  'SNOWC':1,
  'SNOW6H':1,
  'SNOW24H':1,
  'SST':2
}

function query(opts, callback) {
  
  var qcsel = opts.qc ? 3 : 1,
      minbck = opts.window ? Math.round(opts.window/-60) : -120,
      recwin = opts.onlyLatest ? 1 : 4
  
  var url = 'https://madis-data.noaa.gov/madisPublic/cgi-bin/madisXmlPublicDir?rdr=&time=0&minbck='+minbck+'&minfwd=0&recwin='+recwin+'&dfltrsel=0&latll=0.0&lonll=0.0&latur=90.0&lonur=0.0&stanam=&stasel=0&pvdrsel=0&varsel=1&qcsel='+qcsel+'&xml=2&csvmiss=1&nvars=LAT&nvars=LON&nvars=ELEV'
  
  for(var i = 0; i < opts.fields.length; i++) {
    url += '&nvars='+opts.fields[i]
  }
  
  request({
      uri: url,
      auth: {
        user: opts.user,
        pass: opts.pass
      },
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
    
    for(var f = 0; f < fields.length; f++) {
      obs[fields[f]] = parseFloat(cols[8+f])
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

exports.download = function(opts, callback) {
  
  // Sanity check opts params
  if(!opts.user || !opts.pass) {
    return callback(new Error('No user/pass specified.'))
  }
  
  if(!opts.fields || opts.fields.length == 0) {
    return callback(new Error('No fields specified.'))
  }
  
  for(var i = 0; i < opts.fields.length; i++) {
    if(!FIELDS[opts.fields[i]]) {
      return callback(new Error('Invalid field: '+opts.fields[i]))
    }
  }
  
  // Query the NOAA server
  var todo = 1,
      observations, observations_qc
  
  var done = function(err, source_opts, obs) {
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
            if(FIELDS[f] == 2 && observations_qc[s]) {
              observations[s][f] = observations_qc[s][f]
            }
          }
        }
      
      } else if(observations_qc) {
        observations = observations_qc
      }
      
      // Convert to array and finish
      var obs_array = []
      for(var s in observations) {
        obs_array.push(observations[s])
      }
      obs_array.sort(function(a, b) {return b.timestamp - a.timestamp})
      
      return callback(null, obs_array)
    }
  }
  
  // If QC is on, we need to query twice to fill in missing data,
  // because MADIS is stupid (or I am).
  if(opts.qc) {
    
    // Do any of the fields actually not exist in QC results?
    var query_for_non_qc = false
    for(var i = 0; i < opts.fields.length; i++) {
      if(FIELDS[opts.fields[i]] == 1) {
        query_for_non_qc = true
        break
      }
    }
    
    if(query_for_non_qc) {
      todo++
      
      // FIXME: this is an ugly way to clone an object
      var opts2 = JSON.parse(JSON.stringify(opts))
      opts2.qc = false
      query(opts2, done)
    }
  }
  
  query(opts, done)
}
