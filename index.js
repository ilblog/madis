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
  'PCP5M': {qc:false},
  'PCP15M': {qc:false},
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
  'PCPRATE': {qc:false},
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
  'SKYCVLB': {qc:false, numCols:6},
  'SKYCOV': {qc:false, type:'text', numCols:6},
  'PCPTYPE': {qc:false, numCols:2}
}


function query(opts, callback) {
  
  var qcsel = opts.qc ? 3 : 1,
      minbck = opts.window ? Math.round(opts.window/-60) : -120,
      recwin = opts.onlyLatest ? 1 : 4
  
  var url = 'https://madis-data.ncep.noaa.gov/madisPublic1/cgi-bin/madisXmlPublicDir?rdr=&time=0&minbck='+minbck+'&minfwd=0&recwin='+recwin+'&dfltrsel=0&latll=0.0&lonll=0.0&latur=90.0&lonur=0.0&stanam=&stasel=0&pvdrsel=0&varsel=1&qcsel='+qcsel+'&xml=2&csvmiss=1&nvars=LAT&nvars=LON&nvars=ELEV'
  
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
      return callback(new Error('Invalid field: '+opts.fields[i]+'. If it is valid, please add it to the FIELDS array.'))
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
            if(FIELDS[f] && FIELDS[f].qc && observations_qc[s] && observations_qc[s][f] != null && !isNaN(observations_qc[s][f])) {
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
      query(opts2, done)
    }
  }
  
  query(opts, done)
}
