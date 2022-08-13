var http = require('http'),
  request = require('request'),
  encoding = require('encoding'),
  express = require('express'),
  app = (module.exports.app = express()),
  session = require('express-session')({
    secret: 'keyboard cat',
    resave: true,
    saveUninitialized: true,
    cookie: { maxAge: 60000 }
  }),
  socketsParams = {},
  custom = require('./config'),
  maps = require('./maps'),
  sectors = {}

//sharedsession = require("express-socket.io-session");

//({
//	secret: "my-secret",
//	resave: true,
//	saveUninitialized: true
//})

try {
  app.listen(6031, function () {
    console.log('Express app listening on port 6031!')
  })
} catch (e) {
  console.log(
    'Ошибка запуска сервера, возможно копия уже запущена на данном порту!'
  )
  exit(0)
}

app.use(express.static(__dirname + '/tdclient'))
//app.use(session);
// Use the session middleware
app.use(session)
//app.use(express.cookieDecoder());
//app.use(express.session());

var server = http.createServer(app)
var io = require('socket.io')(server) //pass a http.Server instance
//io.use(sharedsession(session, {
//	autoSave: true
//}));
server.listen(8085)
console.log(
  'Сервер диспетчерских приложений TaxiDispatcher запущен на порту 8085...'
)

var sql = require('mssql')
var clientsLimit = 50
var clientsCount = 0

var config = custom.config

//модуль интеграции с Астериском (информатор)
;(function () {
  if (!custom.useAMIClient) {
    console.log('AMI-client disable.')
    return
  }

  var connectionAMI,
    createConnection = function () {
      connectonAttempts++
      if (connectonAttempts > 5) {
        return
      }

      connectionAMI = createAMIDBConnPool(config, function () {
        console.log('Success db-connection of ami module!')
        initAMI()
      })
    },
    connectonAttempts = 0

  createConnection()

  function createAMIDBConnPool(connConfig, callBack) {
    return new sql.ConnectionPool(connConfig, function (err) {
      // ... error checks
      if (err) {
        console.log('Err of create ami db pool' + err.message) // Canceled.
        console.log(err.code)
        console.log('Next connection attempt after 60 sec...')
        setTimeout(createConnection, 60000)
      } else {
        callBack && callBack()
      }
    })
  }

  function findOrderByPhone(phone, callback) {
    queryRequest(
      "SELECT TOP 1 ord.BOLD_ID, (CAST(dr.avar1 as varchar(20)) + ',' + CAST(dr.avar2 as varchar(20)) + ',' + CAST(dr.avar3 as varchar(20)) + ',' + CAST(dr.avar4 as varchar(20)) + ',' + CAST(dr.avar5 as varchar(20)) + ',' + CAST(dr.avar6 as varchar(20)) + ',' + CAST(dr.avar7 as varchar(20)) + ',' + CAST(dr.avar8 as varchar(20)) + ',' + CAST(dr.avar9 as varchar(20)) + ',') as vids FROM Zakaz ord INNER JOIN Voditelj dr ON ord.vypolnyaetsya_voditelem = dr.BOLD_ID WHERE   '" +
        phone +
        "' LIKE ('%' + ord.Telefon_klienta + '%') AND REMOTE_SET >= 0 AND REMOTE_SET <=8 AND Zavershyon = 0 AND Arhivnyi = 0;",
      function (recordset) {
        if (recordset && recordset.recordset && recordset.recordset.length) {
          var vids = recordset.recordset[0].vids
          callback(vids)
        }
      },
      function (err) {
        console.log('Err of findOrderByPhone! ' + err)
      },
      connectionAMI
    )
  }

  function initAMI() {
    var AmiIo = require('ami-io'),
      SilentLogger = new AmiIo.SilentLogger(), //use SilentLogger if you just want remove logs
      amiio = AmiIo.createClient(custom.amiConfig)
    //amiio2 = new AmiIo.Client({ logger: SilentLogger });
    amiio.useLogger(SilentLogger)

    //Both of this are similar

    amiio.on('incorrectServer', function () {
      amiio.logger.error(
        'Invalid AMI welcome message. Are you sure if this is AMI?'
      )
      //process.exit();
    })
    amiio.on('connectionRefused', function () {
      amiio.logger.error('Connection refused.')
      //process.exit();
    })
    amiio.on('incorrectLogin', function () {
      amiio.logger.error('Incorrect login or password.')
      //process.exit();
    })
    amiio.on('event', function (event) {
      var eventName = event.event
      if (
        ['Newexten', 'VarSet', 'InvalidAccountID', 'SuccessfulAuth'].indexOf(
          eventName
        ) < 0
      ) {
        console.log('AMI: ' + eventName)
        if (['Newchannel'].indexOf(eventName) >= 0) {
          //NewConnectedLine, DialBegin
          //console.log(event);
          if (event.channel && event.channel.indexOf('SIP/SIP988') >= 0) {
            if (event.calleridnum && event.calleridnum.length >= 5) {
              console.log('Autoinformator call')

              var callbackRedirect = function () {
                  redirectCall(event)
                },
                callbackFindOrder = function (vids) {
                  setChannelVar(event, 'vids', vids, callbackRedirect)
                }

              findOrderByPhone(event.calleridnum, callbackFindOrder)

              function setChannelVar(channelEvent, varName, value, callback) {
                var actionSetVar = new AmiIo.Action.SetVar()
                actionSetVar.Channel = channelEvent.channel
                actionSetVar.Variable = varName
                actionSetVar.Value = value
                amiio.send(actionSetVar, function (err, data) {
                  if (err) {
                    //err will be event like OriginateResponse if (#response !== 'Success')
                    console.log(err)
                  } else {
                    callback()
                    //data is event like OriginateResponse if (#response === 'Success')
                  }
                })
              }

              function redirectCall(channelEvent) {
                var actionRedirect = new AmiIo.Action.Redirect()
                actionRedirect.Channel = channelEvent.channel
                actionRedirect.Context = 'from-trunk'
                actionRedirect.Exten = '102'
                actionRedirect.Priority = 1

                amiio.send(actionRedirect, function (err, data) {
                  if (err) {
                    //err will be event like OriginateResponse if (#response !== 'Success')
                    console.log(err)
                  } else {
                    //data is event like OriginateResponse if (#response === 'Success')
                  }
                })
              }
            }
          }
        }
      }
    })
    amiio.connect()
    amiio.on('connected', function () {})
  }
})()

// Access the session as req.session
app.get('/sess', function (req, res, next) {
  if (req.session.views) {
    req.session.views++
    res.setHeader('Content-Type', 'text/html')
    res.write('<p>views: ' + req.session.views + '</p>')
    res.write('<p>expires in: ' + req.session.cookie.maxAge / 1000 + 's</p>')
    console.log('ttt2: ' + JSON.stringify(req.session))
    res.end()
  } else {
    req.session.views = 1
    console.log('ttt1: ' + JSON.stringify(req.session))
    res.end('welcome to the session demo. refresh!')
  }
})

app.get('/', function (req, res) {
  req.session.message = 'Hello World'
})

app.get('/twilio-sip-call', function (req, res) {
  res.setHeader('Content-Type', 'text/html')
  res.write('<p>views: </p>')
  res.end()
})

function findClientsSocket(roomId, namespace) {
  var res = [],
    ns = io.of(namespace || '/') // the default namespace is "/"

  if (ns) {
    for (var id in ns.connected) {
      if (roomId) {
        var index = ns.connected[id].rooms.indexOf(roomId)
        if (index !== -1) {
          res.push(ns.connected[id])
        }
      } else {
        //console.log('[[' + JSON.stringify(io.sockets) + ']]');
        res.push(ns.connected[id])
      }
    }
  }
  return res
}

function checkSocketClients() {
  var currentDate = '[' + new Date().toUTCString() + '] ',
    clcnt = 0,
    socketId
  console.log(currentDate)
  var resC = findClientsSocket(),
    socketsIds = []
  for (i = 0; i < resC.length; i++) {
    //console.log(Object.keys(resC[i]));
    console.log(resC[i].id)
    clcnt++
    socketsIds.push(resC[i].id)
  }

  for (socketId in socketsParams) {
    if (socketsIds.indexOf(socketId) < 0) {
      socketsParams[socketId] = {}
    }
  }

  clientsCount = clcnt
  return false
}

function hasSocketWithUserId(userId) {
  var hasSocket = false,
    socketId

  for (socketId in socketsParams) {
    if (socketsParams[socketId].userId === userId) {
      hasSocket = true
      break
    }
  }

  return hasSocket
}

setInterval(checkSocketClients, 60000)

function sendAPIRequest(params, success, fail, options) {
  request(
    Object.assign(
      {
        method: 'GET'
      },
      params
    ),
    function (err, res, body) {
      if (err) {
        console.log(err)
        fail && fail(options)
        return
      }

      if (!body) {
        console.log('No body!')
        fail && fail(options)
        return
      }

      console.log(body)

      try {
        success && success(JSON.parse(body), options)
      } catch (e) {
        console.log(
          'Error of parsing json: ' +
            '\n' +
            JSON.stringify(params) +
            '\n' +
            e.message +
            ' ,line: ' +
            JSON.stringify(e)
        )
        fail && fail(options)
        return
      }
    }
  )
}

function buildRoute(data, orderId, socketId) {
  var routeUrl =
    'http://routes.maps.sputnik.ru/osrm/router/viaroute?loc=' +
    data[0].lat +
    ',' +
    data[0].lon +
    '&loc=' +
    data[1].lat +
    ',' +
    data[1].lon
  console.log('routeUrl: ' + routeUrl)
  sendAPIRequest(
    {
      url: routeUrl
    },
    buildRouteCallback,
    null,
    {
      minLat: -300,
      minLon: -300,
      maxLat: 300,
      maxLon: 300,
      data: data,
      orderId: orderId,
      socketId: socketId
    }
  )
}

var connectionGlobal = createDBConnPool(config)

function buildRouteCallback(data, options) {
  var orderId = options.orderId
  if (orderId <= 0) {
    var i, j
    for (j in [0, 1]) {
      for (i in sectors) {
        sector = sectors[i]
        if (
          maps.isPointInsidePolygon(
            sector.coords,
            options.data[j].lon,
            options.data[j].lat
          )
        ) {
          options.data[j].sector_id = i
          options.data[j].sector_name = sector.name
          break
        }
      }
    }

    data['custom_options'] = options
    socketsParams[options.socketId]['socket'].emit('get-route-result', data)
  } else if (data && data.route_summary) {
    var routeSummary = data.route_summary
    queryRequest(
      'UPDATE Zakaz SET ' +
        ' route_distance = ' +
        (routeSummary.total_distance || 0) +
        ', route_time = ' +
        (routeSummary.total_time || 0) +
        ' WHERE BOLD_ID = ' +
        orderId,
      function (recordset) {
        console.log(
          'Success order set route build parameters!' +
            routeSummary.total_time +
            '=================' +
            JSON.stringify(routeSummary)
        )
      },
      function (err) {
        console.log('Err of order set route build parameters!')
      },
      connectionGlobal
    )
  }
}

function createDBConnPool(connConfig, callBack) {
  return new sql.ConnectionPool(connConfig, function (err) {
    // ... error checks
    if (err) {
      console.log('Err of create db pool: ' + err.message) // Canceled.
      console.log(err.code)
    } else {
      callBack && callBack()
    }
  })
}

//модуль определения координат и сектора заказа по его адресу
;(function () {
  var isActiveDetecting = false,
    geocodeAttemptsCnt = 0,
    defaultGeocodingPrefix = '',
    enableAutoSectorDetect = false,
    enableAutoBuildRoute = false,
    connectionTasks,
    createConnection = function () {
      connectonAttempts++
      if (connectonAttempts > 5) {
        return
      }

      connectionTasks = createTasksDBConnPool(config, function () {
        console.log('Success db-connection of plan tasks module!')
        checkAutoSectorDetectSettings()
      })
    },
    connectonAttempts = 0,
    bbox = {
      minLat: false,
      minLon: false,
      maxLat: false,
      maxLon: false
    }

  createConnection()

  function createTasksDBConnPool(connConfig, callBack) {
    return new sql.ConnectionPool(connConfig, function (err) {
      // ... error checks
      if (err) {
        console.log('Err of create tasks db pool' + err.message) // Canceled.
        console.log(err.code)
        console.log('Next connection attempt after 60 sec...')
        setTimeout(createConnection, 60000)
      } else {
        callBack && callBack()
      }
    })
  }

  function checkAutoSectorDetectSettings() {
    queryRequest(
      'SELECT TOP 1 auto_detect_sector_by_addr, geocode_default_prefix, ' +
        "auto_build_route FROM Objekt_vyborki_otchyotnosti WHERE Tip_objekta='for_drivers';",
      function (recordset) {
        if (
          recordset &&
          recordset.recordset &&
          recordset.recordset.length &&
          recordset.recordset.length == 1
        ) {
          var settingsList = recordset.recordset
          //console.log(sectorCoordsList);
          settingsList.forEach(function (setting) {
            enableAutoSectorDetect = setting.auto_detect_sector_by_addr
            defaultGeocodingPrefix = setting.geocode_default_prefix
            enableAutoBuildRoute = setting.auto_build_route
          })
        }

        if (enableAutoSectorDetect) {
          maps.getSectorsCoordinates(
            sectors,
            bbox,
            connectionTasks,
            function () {
              setInterval(geocodeOrderAddresses, 3000)
            }
          )
        } else {
          console.log('Auto detect sector is off! Next check after 60 sec...')
          setTimeout(checkAutoSectorDetectSettings, 60000)
        }
      },
      function (err) {
        setTimeout(checkAutoSectorDetectSettings, 60000)
        console.log('Err of check auto detect sector settings! ' + err)
        console.log('Next attempt after 60 sec...')
      },
      connectionTasks
    )
  }

  function geocodeOrderAddresses() {
    //console.log('Check orders for geocoded...');
    if (isActiveDetecting && geocodeAttemptsCnt < 5) {
      geocodeAttemptsCnt++
      console.log('Sector detect is active... out')
      return
    }

    geocodeAttemptsCnt = 0

    isActiveDetecting = true
    queryRequest(
      "SELECT ord.BOLD_ID, ord.Adres_vyzova_vvodim, ord.district_id, ISNULL(dis.address, '') as geocode_addr, " +
        ' ISNULL(dis.default_sector_id, 0) as default_sector_id, ord.end_adres, ' +
        " (CASE WHEN (ord.failed_adr_coords_detect = 0 AND ord.detected_sector = -1 AND LEN(ISNULL(ord.Adres_vyzova_vvodim,'')) > 2) THEN 1 ELSE 0 END) as is_start " +
        ' FROM Zakaz ord LEFT JOIN DISTRICTS dis ON ord.district_id = dis.id WHERE ord.Zavershyon = 0 ' +
        " AND ord.adr_manual_set = 1 AND (ord.failed_adr_coords_detect = 0 AND ord.detected_sector = -1 AND LEN(ISNULL(ord.Adres_vyzova_vvodim,'')) > 2) OR " +
        " (ord.failed_end_adr_coords_detect = 0 AND ord.detected_end_sector = -1 AND LEN(ISNULL(ord.end_adres,'')) > 2)",
      function (recordset) {
        if (recordset && recordset.recordset && recordset.recordset.length) {
          var orderList = recordset.recordset,
            order,
            isStart,
            sendAddress
          if (
            bbox.minLat === false ||
            bbox.minLon === false ||
            bbox.maxLat === false ||
            bbox.maxLon === false
          ) {
            isActiveDetecting = false
            console.log('Missing bbox!')
            return
          }

          //orderList.forEach(function(order) {
          order = orderList[0]
          isStart = order.is_start ? true : false
          sendAddress = isStart ? order.Adres_vyzova_vvodim : order.end_adres
          if (order) {
            console.log(
              (isStart ? 'Адрес отправления: ' : 'Адрес назначения: ') +
                (order.district_id &&
                order.district_id > 0 &&
                order.geocode_addr &&
                !isStart
                  ? order.geocode_addr
                  : defaultGeocodingPrefix) +
                ',' +
                sendAddress
            )

            if (
              bbox.minLat === false ||
              bbox.minLon === false ||
              bbox.maxLat === false ||
              bbox.maxLon === false
            ) {
              isActiveDetecting = false
              setFailOrderSectDetect(
                order.BOLD_ID,
                order.default_sector_id,
                isStart
              )
              console.log('Missing bbox!')
              return
            }

            sendAPIRequest(
              {
                url: 'http://search.maps.sputnik.ru/search/addr',
                qs: {
                  q:
                    (order.district_id &&
                    order.district_id > 0 &&
                    order.geocode_addr
                      ? order.geocode_addr
                      : defaultGeocodingPrefix) +
                    ',' +
                    sendAddress
                  // && !isStart
                  //blat: bbox.minLat,
                  //blon: bbox.maxLon,
                  //tlat: bbox.maxLat,
                  //tlon: bbox.minLon,
                  //strict: true
                }
              },
              detectSectorOnGeocodeData,
              geocodeApiFailCallback,
              {
                orderId: order.BOLD_ID,
                defaultSectorId: order.default_sector_id,
                districtId: order.district_id,
                minLat: bbox.minLat,
                minLon: bbox.minLon,
                maxLat: bbox.maxLat,
                maxLon: bbox.maxLon,
                isStart: isStart
              }
            )
          } else {
            isActiveDetecting = false
            console.log('Missing order!')
          }
        } else {
          isActiveDetecting = false
        }
      },
      function (err) {
        isActiveDetecting = false
        console.log('Err of geocodeOrderAddresses request! ' + err)
      },
      connectionTasks
    )
  }

  function geocodeApiFailCallback(options) {
    isActiveDetecting = false
    setFailOrderSectDetect(
      options.orderId,
      options.defaultSectorId,
      options.isStart
    )
  }

  function detectSectorOnGeocodeData(data, options) {
    var sector,
      districtId = (options && options.districtId) || -1,
      orderId = options && options.orderId,
      isDetected = false,
      defaultSectorId = options && options.defaultSectorId,
      isStart = options.isStart

    var parseResult = maps.parseCoordinatesFromGeocodeData(data, options)
    if (parseResult.emptyAddress) {
      setFailOrderSectDetect(orderId, defaultSectorId, options.isStart)
    }

    var pointLat = parseResult.pointLat,
      pointLon = parseResult.pointLon,
      withoStreetLat = parseResult.withoutStreetPointLat,
      withoStreetLon = parseResult.withoutStreetPointLon

    /*featuresList = featureCollection && featureCollection.features && featureCollection.features.length && featureCollection.features.filter(function(feat) { return feat.type === 'Feature'; }),
			feature = featuresList && featuresList.length && featuresList[0],
			featureGeometries = feature && feature.geometry && feature.geometry.type && feature.geometry.type === 'GeometryCollection' && feature.geometry.geometries,*/

    /*pointGeometryList = featureGeometries && featureGeometries.length && featureGeometries.filter(function(geom) { return geom.type === 'Point'; }),
			geoPoint = pointGeometryList && pointGeometryList.length && pointGeometryList[0],
			pointCoordinates = geoPoint && geoPoint.coordinates,*/
    //pointCoordinates = getAddrPointsByFeatureGeometries(featureGeometries, options),

    if (pointLat && pointLon && orderId) {
      console.log('Point lat=' + pointLat + ', lon=' + pointLon)
      for (i in sectors) {
        sector = sectors[i]

        if (districtId > 0 && sector.districtId !== districtId) {
          console.log(sector.districtId + '+++' + districtId)
          continue
        }

        if (maps.isPointInsidePolygon(sector.coords, pointLon, pointLat)) {
          console.log(
            'Point lat=' +
              pointLat +
              ', lon=' +
              pointLon +
              ' inside to ' +
              sector.name
          )
          queryRequest(
            'UPDATE Zakaz SET ' +
              (isStart ? 'detected_sector' : 'detected_end_sector') +
              ' = ' +
              i +
              (isStart
                ? ", adr_detect_lat = '" +
                  pointLat +
                  "', adr_detect_lon = '" +
                  pointLon +
                  "' " +
                  ', depr_lat = ' +
                  pointLat +
                  ', depr_lon = ' +
                  pointLon
                : ', dest_lat = ' + pointLat + ', dest_lon = ' + pointLon) +
              ' WHERE BOLD_ID = ' +
              orderId,
            function (recordset) {
              if (enableAutoBuildRoute) {
                checkRouteBuild(orderId)
              }
              isActiveDetecting = false
            },
            function (err) {
              isActiveDetecting = false
              setFailOrderSectDetect(orderId, defaultSectorId, options.isStart)
              console.log('Err of order detected sector assign request! ' + err)
            },
            connectionTasks
          )

          isDetected = true
          break
        }
      }

      if (!isDetected) {
        queryRequest(
          'UPDATE Zakaz SET ' +
            (isStart ? 'detected_sector' : 'detected_end_sector') +
            ' = -1 ' +
            (isStart
              ? ", adr_detect_lat = '" +
                pointLat +
                "', adr_detect_lon = '" +
                pointLon +
                "' " +
                ', depr_lat = ' +
                pointLat +
                ', depr_lon = ' +
                pointLon
              : ', dest_lat = ' + pointLat + ', dest_lon = ' + pointLon) +
            ' WHERE BOLD_ID = ' +
            orderId,
          function (recordset) {
            if (enableAutoBuildRoute) {
              checkRouteBuild(orderId)
            }
          },
          function (err) {
            console.log('Err of order detected sector assign request! ' + err)
          },
          connectionTasks
        )
      }
    } else if (withoStreetLat && withoStreetLon && orderId) {
      ////', adr_detect_lat = \'' + withoStreetLat + '\', adr_detect_lon = \'' + withoStreetLon + '\' ' +
      console.log(
        'Point without street lat=' + withoStreetLat + ', lon=' + withoStreetLon
      )
      queryRequest(
        'UPDATE Zakaz SET ' +
          (isStart
            ? 'failed_adr_coords_detect'
            : 'failed_end_adr_coords_detect') +
          ' = 1' +
          (isStart
            ? ", adr_detect_lat = '" +
              withoStreetLat +
              "', adr_detect_lon = '" +
              withoStreetLon +
              "' " +
              ', depr_lat = ' +
              withoStreetLat +
              ', depr_lon = ' +
              withoStreetLon
            : ', dest_lat = ' +
              withoStreetLat +
              ', dest_lon = ' +
              withoStreetLon) +
          ' WHERE BOLD_ID = ' +
          orderId,
        function (recordset) {
          isActiveDetecting = false
        },
        function (err) {
          isActiveDetecting = false
          setFailOrderSectDetect(orderId, defaultSectorId, options.isStart)
          console.log('Err of order set detect coords without streets! ' + err)
        },
        connectionTasks
      )
    }

    //console.log('333');
    if (!isDetected && orderId) {
      setFailOrderSectDetect(orderId, defaultSectorId, options.isStart)
    } else {
      isActiveDetecting = false
    }
  }

  function checkRouteBuild(orderId) {
    queryRequest(
      'SELECT ord.BOLD_ID, ord.Adres_vyzova_vvodim, ord.district_id, ord.end_adres, ' +
        ' ord.dest_lat, ord.dest_lon, ord.depr_lat, ord.depr_lon ' +
        ' FROM Zakaz ord WHERE ord.BOLD_ID = ' +
        orderId +
        ' AND ord.Zavershyon = 0 ' +
        ' AND ord.adr_manual_set = 1 AND ord.failed_adr_coords_detect = 0 AND ' +
        " ord.detected_sector > 0 AND LEN(ISNULL(ord.Adres_vyzova_vvodim,'')) > 2 AND " +
        ' ord.failed_end_adr_coords_detect = 0 AND ord.detected_end_sector > 0 AND ' +
        " LEN(ISNULL(ord.end_adres,'')) > 2 AND ord.dest_lat > 0 AND ord.dest_lon > 0 " +
        ' AND ord.depr_lat > 0 AND ord.depr_lon > 0',
      function (recordset) {
        //console.log('111');
        if (recordset && recordset.recordset && recordset.recordset.length) {
          var orderList = recordset.recordset,
            order,
            isStart,
            sendAddress

          order = orderList[0]
          buildRoute(
            [
              {
                lat: order.depr_lat,
                lon: order.depr_lon
              },
              {
                lat: order.dest_lat,
                lon: order.dest_lon
              }
            ],
            orderId,
            null
          )
        }
      },
      function (err) {
        isActiveDetecting = false
        console.log(
          'Err of checkRouteBuild geocodeOrderAddresses request! ' + err
        )
      },
      connectionTasks
    )
  }

  function setFailOrderSectDetect(orderId, sector_id, isStart) {
    queryRequest(
      'UPDATE Zakaz SET ' +
        (isStart
          ? 'failed_adr_coords_detect'
          : 'failed_end_adr_coords_detect') +
        ' = 1 ' +
        (sector_id
          ? ' , ' +
            (isStart ? 'detected_sector' : 'detected_end_sector') +
            ' = ' +
            sector_id +
            ' '
          : ' ') +
        ' WHERE BOLD_ID = ' +
        orderId,
      function (recordset) {
        isActiveDetecting = false
      },
      function (err) {
        isActiveDetecting = false
        console.log('Err of order FAIL detected sector SET FLAG request!')
      },
      connectionTasks
    )
  }
})()

function queryRequest(sqlText, callbackSuccess, callbackError, connection) {
  var request = new sql.Request(connection)
  request.query(sqlText, function (err, recordset) {
    if (err) {
      console.log(err.message)
      console.log(err.code)
      callbackError && callbackError(err)
    } else {
      callbackSuccess && callbackSuccess(recordset)
    }
  })
}

io.sockets.on('connection', function (socket) {
  console.log('New sock id: ' + socket.id)
  socketsParams[socket.id] = {}
  var reqTimeout = 0
  var reqCancelTimeout = 0
  var stReqTimeout = 0
  var authTimeout = 0
  var clientActiveTime = 0
  var socketDBConfig = config
  var webProtectedCode = '' //socket.handshake.session.webProtectedCode || '';
  var userId = -1

  //console.log('fff: ' + JSON.stringify(socket.handshake.session));

  //socketDBConfig.user = '';//socket.handshake.session.user || '';
  //socketDBConfig.password = '';//socket.handshake.session.password || '';

  var condition = {
      orders: {
        Zavershyon: 0,
        Arhivnyi: 0,
        Predvariteljnyi: 0
      }
    },
    condDependencies = [
      {
        type: 'dataSelect',
        staticExpression: 'TOP',
        Arhivnyi: 1,
        Zavershyon: 1,
        Predvariteljnyi: 'NONE',
        injectExpression: 'TOP 20'
      },
      {
        type: 'dataSelect',
        staticExpression: 'ORDER',
        Arhivnyi: 1,
        Zavershyon: 1,
        Predvariteljnyi: 'NONE',
        injectExpression: 'ORDER BY BOLD_ID DESC'
      },
      {
        type: 'dataSelect',
        staticExpression: 'ORDER',
        Arhivnyi: 0,
        Zavershyon: 0,
        Predvariteljnyi: 'NONE',
        injectExpression: 'ORDER BY BOLD_ID DESC'
      },
      {
        type: 'dataUpdate',
        staticExpression: 'ORDER_DRNUM',
        Pozyvnoi_ustan: 'INJECT',
        id: 'INJECT',
        otpuskaetsya_dostepcherom: 'INJECT',
        adr_manual_set: 'INJECT',
        injectExpression:
          'EXEC	[dbo].[AssignDriverByNumOnOrder] @order_id = ${options.id}, @driver_num = ${options.Pozyvnoi_ustan}, @user_id = ${options.otpuskaetsya_dostepcherom}, @count = 0'
      } //,
      /*{
				type: 'dataUpdate',
				staticExpression: 'ORDER_BAD_COMMENT',
				order_bad_comment: 'INJECT',
				id: 'INJECT',
				otpuskaetsya_dostepcherom: 'INJECT',
				injectExpression: 'EXEC	[dbo].[AddClientToBlackList] @order_id = ${options.id}, @comment = \'${options.order_bad_comment}\', @user_id = ${options.otpuskaetsya_dostepcherom}, @count = 0'
			}*/
    ],
    entityDependencies = {
      orders: [
        {
          type: 'base',
          list: 'ActiveOrders'
        },
        {
          type: 'relation',
          list: 'Sektor_raboty sr LEFT JOIN Spravochnik sp ON sr.BOLD_ID = sp.BOLD_ID',
          link: 'order_sect'
        },
        {
          type: 'relation',
          list: 'DISTRICTS',
          link: 'order_district'
        },
        {
          type: 'relation',
          list: 'PRICE_POLICY',
          link: 'order_tariff_plan'
        }
      ]
    }

  function getDependListData(entityDependenciesList, callBack, dependData) {
    if (entityDependenciesList && entityDependenciesList.length) {
      var request = new sql.Request(connection)
      request.query(
        'select * FROM ' + entityDependenciesList[0].list, //
        function (err, recordset) {
          if (err) {
            console.log(err)
            callBack([])
            return
          }
          dependData[entityDependenciesList[0].link] = recordset.recordset

          if (entityDependenciesList.length > 1) {
            entityDependenciesList.splice(0, 1)
            getDependListData(entityDependenciesList, callBack, dependData)
          } else {
            callBack(dependData)
          }
        }
      )
    }
  }

  function getEntityDependData(entity, callBack) {
    var dependData = {},
      entityDependenciesList =
        entityDependencies[entity] &&
        entityDependencies[entity].filter(function (dependency) {
          return dependency.type === 'relation'
        })

    getDependListData(entityDependenciesList, callBack, dependData)
    //callBack([]);
  }

  function getDependenceInject(options, dependencyType) {
    if (!options) {
      return ''
    }

    //if (!dependencyType) {
    //	dependencyType = 'dataSelect';
    //}

    var dependencies = condDependencies,
      dependStr = '',
      isInjectedOptions = false,
      i

    console.log('len=' + dependencies.length)
    //console.log('dependencies: ' + dependencies);
    console.log('options: ')
    console.log(options)
    //dependencies = dependencies.filter(function(dependency) {
    //	return dependency.type == dependencyType;
    //});

    for (i in options) {
      dependencies = dependencies.filter(function (dependency) {
        return (
          typeof dependency[i] !== 'undefined' &&
          (dependency[i] === options[i] ||
            dependency[i] === 'NONE' ||
            dependency[i] === 'INJECT')
        )
      })

      //console.log(i + ': ' + options[i] +
      //	', len=' + dependencies.length);
      if (!isInjectedOptions) {
        isInjectedOptions |= dependencies.filter(function (dependency) {
          return (
            typeof dependency[i] !== 'undefined' && dependency[i] === 'INJECT'
          )
        }).length
      }
    }

    isInjectedOptions && console.log('is injected!')

    dependencies.forEach(function (dependency) {
      if (
        !isInjectedOptions ||
        validateDependencyOptions(dependency, options)
      ) {
        dependStr += isInjectedOptions
          ? eval('`' + dependency.injectExpression + '`')
          : dependency.injectExpression
      }
    })

    //console.log('dependStr: [' + dependStr + ']');
    return dependStr
  }

  function validateDependencyOptions(dependency, options) {
    //console.log(dependency);
    //console.log(options);
    for (i in dependency) {
      if (
        dependency[i] === 'INJECT' &&
        options &&
        typeof options[i] === 'undefined'
      ) {
        return false
      }
      //console.log('depend i=' + i + '[' + (typeof options[i]));
    }
    return true
  }

  function decReqTimeout() {
    if (reqTimeout > 0) reqTimeout--
    if (stReqTimeout > 0) stReqTimeout--
    if (reqCancelTimeout > 0) reqCancelTimeout--
    if (authTimeout > 0) authTimeout--
  }

  setInterval(decReqTimeout, 1000)

  if (clientsCount + 1 > clientsLimit) {
    socket.emit('server overload', { me: -1 })
    try {
      socket.disconnect('server overload')
    } catch (e) {
      console.log('error socket disconnect')
    }
    try {
      socket.close()
    } catch (e) {
      console.log('error socket close')
    }
    return
  } else {
    console.log('client connect, num=' + clientsCount)
    clientsCount++
  }

  var connection = createDBConnPool(socketDBConfig)

  /*function createDBConnPool(connConfig, callBack) {
		return new sql.ConnectionPool(connConfig, function (err) {
			// ... error checks
			if (err) {
				console.log('Err of create db pool: ' + err.message);                      // Canceled.
				console.log(err.code);
			} else {
				callBack && callBack();
			}
		});
	}*/

  function dependencyExpression(optionsArray) {
    var dependOptions = {}

    optionsArray.forEach(function (optionItem) {
      Object.assign(dependOptions, optionItem)
    })

    //console.log('dependOptions: ');
    //console.log(dependOptions);
    return getDependenceInject(dependOptions)
  }

  function emitData(entity) {
    if (userId < 0) {
      return
    }

    if (entity.indexOf('is_order_data_updates') === 0) {
      console.log('emit is_order_data_updates')
      socket.emit('is_order_data_updates', {
        userId: userId
      })
    } else if (
      entity.indexOf('orders') === 0 &&
      entity.indexOf('orders_coordinates') !== 0
    ) {
      var baseCallback = function (dependData) {
        //console.log('===========>>>' + dependData);
        var request = new sql.Request(connection),
          whereClause =
            ' where (Zavershyon = ' +
            condition.orders.Zavershyon +
            ') AND (Arhivnyi = ' +
            condition.orders.Arhivnyi +
            ')'
        request.query(
          'select ' +
            dependencyExpression([
              { type: 'dataSelect', staticExpression: 'TOP' },
              condition.orders
            ]) +
            ' * FROM ActiveOrders ' +
            whereClause +
            dependencyExpression([
              { type: 'dataSelect', staticExpression: 'ORDER' },
              condition.orders
            ]),
          function (err, recordset) {
            socket.emit('orders', {
              userId: userId,
              orders: recordset && recordset.recordset,
              depends: dependData //{ order_sect: [1,2,3] }
            })
          }
        )
      }
      getEntityDependData('orders', baseCallback)
    } else if (entity.indexOf('drivers') === 0) {
      var whereClause = ' where V_rabote = 1 AND Pozyvnoi > 0'
      queryRequest(
        'SELECT BOLD_ID as id, Pozyvnoi, last_lat, last_lon, Na_pereryve,' +
          ' rabotaet_na_sektore, Zanyat_drugim_disp, ABS(DATEDIFF(minute, LAST_STATUS_TIME, GETDATE())) as status_time FROM Voditelj ' +
          whereClause,
        function (recordset) {
          if (recordset && recordset.recordset) {
            socket.emit('drivers', {
              userId: userId,
              drivers: recordset && recordset.recordset
            })
          }
        },
        function (err) {},
        connection
      )
    } else if (entity.indexOf('orders_coordinates') === 0) {
      var whereClause =
        " where Zavershyon = 0 AND Arhivnyi = 0 AND (NOT (ISNULL(rclient_lat, '') = '' OR ISNULL(rclient_lon, '') = '') OR NOT (ISNULL(adr_detect_lat, '') = '' OR ISNULL(adr_detect_lon, '') = ''))"
      //console.log('orders_coordinates');
      queryRequest(
        "select BOLD_ID as id, (CASE WHEN (ISNULL(rclient_lat, '') <> '') THEN rclient_lat ELSE adr_detect_lat END) as lat, (CASE WHEN (ISNULL(rclient_lat, '') <> '') THEN rclient_lon ELSE adr_detect_lon END) as lon, Adres_vyzova_vvodim as addr, vypolnyaetsya_voditelem FROM Zakaz" +
          whereClause,
        function (recordset) {
          //console.log(recordset.recordset);
          recordset &&
            recordset.recordset &&
            socket.emit('orders_coordinates', {
              userId: userId,
              orders: recordset && recordset.recordset
            })
        },
        function (err) {
          console.log(err)
        },
        connection
      )
    }
  }

  function checkDriversCoordsUpdated() {
    queryRequest(
      'SELECT drivers_coord_updated, orders_coord_updated FROM Personal WHERE (drivers_coord_updated = 1 OR orders_coord_updated = 1) AND BOLD_ID = ' +
        userId,
      function (recordset) {
        if (recordset && recordset.recordset && recordset.recordset.length) {
          var drivers_coord_updated =
              recordset.recordset[0].drivers_coord_updated,
            orders_coord_updated = recordset.recordset[0].orders_coord_updated
          queryRequest(
            'UPDATE Personal SET drivers_coord_updated = 0, orders_coord_updated = 0 WHERE BOLD_ID = ' +
              userId,
            function (recordset) {
              //console.log('emit updated drivers coords');
              drivers_coord_updated && emitData('drivers')
              orders_coord_updated && emitData('orders_coordinates')
            },
            function (err) {},
            connection
          )
        }
      },
      function (err) {},
      connection
    )
  }

  function checkDataUpdated() {
    queryRequest(
      'SELECT has_web_orders_updates, Prover_vodit ' +
        'FROM Personal WHERE has_web_orders_updates = 1 AND BOLD_ID = ' +
        userId,
      function (recordset) {
        if (recordset && recordset.recordset && recordset.recordset.length) {
          console.log('has_web_orders_updates = 1')
          var EstjVneshnieManip = recordset.recordset[0].has_web_orders_updates
          queryRequest(
            'UPDATE Personal SET has_web_orders_updates = 0 WHERE BOLD_ID = ' +
              userId,
            function (recordset) {
              EstjVneshnieManip && emitData('is_order_data_updates')
              //console.log('emit updated order data');
              //EstjVneshnieManip && emitData('orders');
              //EstjVneshnieManip && emitData('orders_coordinates');
            },
            function (err) {},
            connection
          )
        }
      },
      function (err) {},
      connection
    )
  }

  /*socket.on('crud', function (data) {
		console.log(data);
		if (typeof data === 'string') {
			tp = tryParseJSON(data);
			console.log("=======");
			console.log(tp);
			if (tp)
				data = tp;
		}

		if (data && data.entity && data.entity === 'order') {
			emitData('orders');
		}
	});*/

  socket.on('app-state', function (data) {
    //console.log('app-state');
    if (data) {
      //data.orders && console.log('emitData orders');
      //data.drivers && console.log('emitData drivers');
      //data.orders_coordinates && console.log('emitData orders_coordinates');
      data.orders && emitData('orders')

      if (data.drivers && data.orders_coordinates) {
        checkDriversCoordsUpdated()
      } else {
        data.drivers && emitData('drivers')
        data.orders_coordinates && emitData('orders_coordinates')
      }
      checkDataUpdated()
    }
  })

  socket.on('orders-state', function (data) {
    console.log(data)
    if (typeof data === 'string') {
      tp = tryParseJSON(data)
      console.log('=======')
      console.log(tp)
      if (tp) data = tp
    }

    if (data.aspects && data.aspects.length) {
      var aspects = data.aspects
      data = data.states
      console.log(aspects)

      aspects.forEach(function (aspect) {
        eval(aspect + '(data);')
      })
    }

    condition.orders = data
    emitData('orders')
  })

  socket.on('my other event', function (data) {
    console.log(data)
  })

  function tryParseJSON(jsonString) {
    try {
      var o = JSON.parse(jsonString)

      if (o && typeof o === 'object' && o !== null) {
        return o
      }
    } catch (e) {}

    return false
  }

  function identDBConnectCallback() {
    queryRequest(
      'SELECT TOP 1 web_protected_code FROM Objekt_vyborki_otchyotnosti ' +
        " WHERE Tip_objekta = 'for_drivers' AND web_protected_code = '" +
        webProtectedCode +
        "'",
      function (recordset) {
        if (recordset && recordset.recordset && recordset.recordset.length) {
          //socket.handshake.session.webProtectedCode = webProtectedCode;
          //socket.handshake.session.user = socketDBConfig.user;
          //socket.handshake.session.password = socketDBConfig.password;
          //socket.handshake.session.save();

          //console.log('ggg: ' + JSON.stringify(socket.handshake.session));

          queryRequest(
            'SELECT TOP 1 BOLD_ID FROM Personal ' +
              " WHERE Login = '" +
              socketDBConfig.user +
              "'",
            function (recordset) {
              if (
                recordset &&
                recordset.recordset &&
                recordset.recordset.length
              ) {
                userId = recordset.recordset[0].BOLD_ID

                if (hasSocketWithUserId(userId)) {
                  abortConnection('Данный пользователь уже подключен!')
                  return
                }

                socketsParams[socket.id]['userId'] = userId
                socketsParams[socket.id]['socket'] = socket

                emitData('orders')
                console.log('emit drivers')
                emitData('drivers')
                console.log('emit orders_coordinates')
                emitData('orders_coordinates')
                //setInterval(checkDriversCoordsUpdated, 10000);
                //setInterval(checkDataUpdated, 5000);
              }
            },
            function (err) {},
            connection
          )
        }
      },
      function (err) {},
      connection
    )

    if (authTimeout <= 0) {
      authTimeout = 20

      var request = new sql.Request(connection)

      request.input('phone', sql.VarChar(255), data.phone)
      request.output('client_id', sql.Int, data.id)
      request.output('req_trust', sql.Int, 0)
      request.output('isagainr', sql.Int, 0)
      request.output('acc_status', sql.Int, 0)
      request.execute(
        'CheckClientRegistration',
        function (err, recordsets, returnValue) {
          if (err) {
            console.log('Error of CheckClientRegistration:' + err.message)
            console.log('Error code:' + err.code)
          } else {
            var parameters = recordsets.output
            console.log(
              'CheckClientRegistration result client_id=' + parameters.client_id
            )
            socket.emit('auth', {
              client_id: parameters.client_id,
              req_trust: parameters.req_trust,
              isagainr: parameters.isagainr,
              acc_status: parameters.acc_status
            })
          }
        }
      )
    } else console.log('Too many requests from ' + data.phone)
  }

  socket.on('ident', function (data) {
    console.log(data)
    console.log('=======')
    console.log(typeof data)
    if (typeof data === 'string') {
      tp = tryParseJSON(data)
      if (tp) data = tp
    }
    socketDBConfig.user = data.login
    socketDBConfig.password = data.psw
    webProtectedCode = data.code
    connection = createDBConnPool(socketDBConfig, identDBConnectCallback)
  })

  function abortConnection(abortMsg) {
    socket.emit('abort_connection', {
      msg: abortMsg
    })
    connection = null
  }

  function requestAndSendStatus(conn, cid, clphone, direct) {
    if (stReqTimeout <= 0 || direct) {
      stReqTimeout = 20
      var request = new sql.Request(conn)
      request.input('client_id', sql.Int, parseInt(cid))
      //request.input('adres', sql.VarChar(255), encoding.convert('привет мир','CP1251','UTF-8'));
      request.input('phone', sql.VarChar(255), clphone)
      request.input('full_data', sql.Int, 0)
      request.output('res', sql.VarChar(2000), '')
      request.execute(
        'GetJSONRClientStatus',
        function (err, recordsets, returnValue) {
          if (err) {
            console.log(err.message) // Canceled.
            console.log(err.code) // ECANCEL //
          } else {
            var parameters = recordsets.output
            socket.emit('clstat', { cl_status: parameters.res })
          }
        }
      )
    } else {
      console.log('Too many requests from ' + clphone)
    }
  }

  socket.on('status', function (data) {
    //console.log(data);
    //console.log("=======");
    //console.log(typeof data);
    //if (typeof data === 'string') {
    //	tp = tryParseJSON(data);
    //	console.log("=======");
    //	console.log(tp);
    //	if (tp)
    //		data = tp;
    //}

    requestAndSendStatus(connection, data.cid)
    console.log('Status request: ' + JSON.stringify(data))
  })

  var newOrder = function (data) {
    //console.log('newOrder: ====================>>>>>>>>>>>>>>' + JSON.stringify(data));
    queryRequest(
      "EXEC	[dbo].[InsertOrderWithParamsRDispatcher] @adres = N'', @enadres = N'',@phone = N'',@disp_id = -1, @status = 0, @color_check = 0, @op_order = 0, @gsm_detect_code = 0,@deny_duplicate = 0, @colored_new = 0, @ab_num = N'', @client_id = -1, @ord_num = 0,@order_id = 0",
      function (recordset) {
        emitData('orders')
      },
      function (err) {},
      connection
    )
    /*if (reqTimeout <= 0 || true) {
			stReqTimeout = 0;
		} else
			socket.emit('req_decline', {status: "many_new_order_req"});
		reqTimeout = 60;*/
  }

  socket.on('order', function (data) {
    if (userId < 0) {
      return
    }

    //console.log(data);
    //console.log("=======");
    //console.log(typeof data);
    if (typeof data === 'string') {
      tp = tryParseJSON(data)
      //console.log("=======");
      //console.log(tp);
      if (tp) data = tp
    }

    var counter = 0,
      setPhrase = '',
      wherePhrase = ' WHERE BOLD_ID = ',
      conditionQuery = dependencyExpression([
        { type: 'dataUpdate', staticExpression: 'ORDER_DRNUM' },
        data
      ])

    if (!conditionQuery) {
      for (i in data) {
        if (counter > 0) {
          setPhrase +=
            (counter == 1 ? ' ' : ', ') +
            i +
            '=' +
            (typeof data[i] === 'string' ? "'" + data[i] + "'" : data[i])
        } else {
          wherePhrase += data[i]
        }
        counter++
      }
      conditionQuery =
        setPhrase.length && 'UPDATE Zakaz SET ' + setPhrase + wherePhrase
    }

    console.log(conditionQuery)
    conditionQuery.length &&
      queryRequest(
        conditionQuery,
        function (recordset) {
          if (recordset && recordset.recordset && recordset.recordset.length) {
            //emitData('orders');
          }
          emitData('orders')
        },
        function (err) {
          console.log('Err of: ' + conditionQuery)
          emitData('orders')
        },
        connection
      )
  })

  socket.on('get-addr-coords', function (data) {
    console.log('on get-addr-coords: ' + JSON.stringify(data))
    sendAPIRequest(
      {
        url: 'http://search.maps.sputnik.ru/search/addr',
        qs: {
          q: data.address
        }
      },
      detectCoordsByAddr,
      null,
      {
        minLat: -300,
        minLon: -300,
        maxLat: 300,
        maxLon: 300,
        data: data
      }
    )
  })

  function detectCoordsByAddr(data, options) {
    var parseResult = maps.parseCoordinatesFromGeocodeData(data, options)

    var pointLat = parseResult.pointLat,
      pointLon = parseResult.pointLon,
      withoutStreetPointLat = parseResult.withoutStreetPointLat,
      withoutStreetPointLon = parseResult.withoutStreetPointLon

    if (pointLat && pointLon) {
      console.log('Point from front lat=' + pointLat + ', lon=' + pointLon)
      var resultData = {
        lat: pointLat,
        lon: pointLon,
        mode: options.data.mode,
        address: options.data.address
      }
      socket.emit('detected-addr-coords', resultData)
      socket.emit('planned-orders', resultData)
    }
  }

  socket.on('get-route', function (data) {
    buildRoute(data, -1, socket.id)
  })

  socket.on('disconnect', function () {
    socketsParams[socket.id] = {}
    console.log('user disconnected')
    clientsCount--
  })
})
