;(function (angular) {
  'use strict'
  function DriversMapController(
    $scope,
    $element,
    $attrs,
    injectedSocket,
    ascInterface
  ) {
    var ctrl = this,
      icons = {},
      markers = {},
      orders = {},
      orderIcons = {},
      plannedOrders = [null, null],
      plannedOrdersIcons = [null, null],
      isFirstPopup = false

    ctrl.list = []
    ctrl.userId = -1

    //L.sm.apiKey = '<Ваш API-ключ>';
    var map = L.sm.map('map1', {
      center: [44.878773, 37.314388],
      zoom: 14
    })

    /*var myIcon3 = L.icon({ // создаем иконку
			iconUrl: '//maps-js.apissputnik.ru/v0.3/images/marker-icon.png'
		});

		//44.878773, 37.314388 55.85, 37.61
		var myMarker30 = L.sm.marker([55.85, 37.61], {
			icon: myIcon3 // передаем иконку маркеру
		});

		var myIcon4 = L.icon({ // создаем иконку
			iconUrl: '//maps-js.apissputnik.ru/v0.3/images/marker-icon.png'
		});

		var myMarker40 = L.sm.marker([55.75, 37.51], {
			icon: myIcon4 // передаем иконку маркеру
		});

		myMarker30.addTo(map); // добавляем маркер на карту

		setTimeout(step1, 5000);
		//setTimeout('myMarker30.setLatLng([56.05, 37.81]);', 2000);
		//setTimeout('myMarker30.setLatLng([56.15, 37.91]);', 2000);

		function step1() {
			myMarker30.setLatLng([55.95, 37.71]); myMarker30.addTo(map); myMarker40.addTo(map);
		}*/

    injectedSocket.on('planned-orders', function (data) {
      var orderLat = data.lat,
        orderLon = data.lon,
        orderId = data.mode.indexOf('start-adr') >= 0 ? 0 : 1,
        iconFileName = 'man_free'

      if (orderLat && orderLon) {
        if (!plannedOrdersIcons[orderId]) {
          plannedOrdersIcons[orderId] = L.icon({
            // создаем иконку
            iconUrl: '/images/man.png',
            iconSize: [25, 25]
          })
        }

        var orderIcon = new L.DivIcon({
          className: 'my-div-icon',
          html:
            '<img class="imageMarker" src="/images/' +
            iconFileName +
            '.png"/><span class="markerText">' +
            data.address +
            '</span>'
        })
        if (!plannedOrders[orderId]) {
          plannedOrders[orderId] = L.sm.marker([orderLat, orderLon], {
            //orderIcons[order.id], // передаем иконку маркеру
            icon: orderIcon,
            title: '' + data.address
          })
          plannedOrders[orderId].bindPopup('' + data.address)
        } else {
          plannedOrders[orderId].setIcon(orderIcon)
          plannedOrders[orderId].setLatLng([orderLat, orderLon])
        }

        plannedOrders[orderId].addTo(map)
      }
    })

    injectedSocket.on('drivers', function (data) {
      var driverLat,
        driverLon,
        marker,
        driversIds = [],
        driverId
      ctrl.list = data.drivers
      ctrl.userId = data.userId

      ctrl.list.forEach(function (driver) {
        driverLat = driver.last_lat
        driverLon = driver.last_lon
        driversIds.push(parseInt(driver.id, 10))

        if (
          driverLat &&
          driverLon &&
          parseInt(driverLat, 10) > 1 &&
          parseInt(driverLon, 10) > 1
        ) {
          icons[driver.id] =
            driver.status_time > 2 && false
              ? '/images/car_white.png'
              : driver.Zanyat_drugim_disp
              ? '/images/car_red.png'
              : driver.Na_pereryve
              ? '/images/car_blue.png'
              : '/images/car_green.png'

          if (!markers[driver.id]) {
            markers[driver.id] = L.sm.marker([driverLat, driverLon], {
              //icons[driver.id], // передаем иконку маркеру
              icon: new L.DivIcon({
                className: 'my-div-icon',
                html:
                  '<img class="imageMarker" src="' +
                  icons[driver.id] +
                  '"/>' +
                  '<span class="markerText">' +
                  driver.Pozyvnoi +
                  '</span>'
              }),
              title: '' + driver.Pozyvnoi,
              iconClassName: 'icon-w12h20',
              iconType: driver.Zanyat_drugim_disp ? 'alt2' : 'alt3'
              //popup: '' + driver.Pozyvnoi, // здесь может быть код HTML или DOM-элемент
              //popupOptions: {
              //	open: true
              //}
            })
            markers[driver.id].bindPopup('' + driver.Pozyvnoi)
          } else {
            markers[driver.id].setIcon(
              new L.DivIcon({
                className: 'my-div-icon',
                html:
                  '<img class="imageMarker" src="' +
                  icons[driver.id] +
                  '"/>' +
                  '<span class="markerText">' +
                  driver.Pozyvnoi +
                  '</span>'
              })
            )
            markers[driver.id].setLatLng([driverLat, driverLon])
            markers[driver.id].setOpacity(1)
          }

          markers[driver.id].addTo(map)

          if (!isFirstPopup) {
            isFirstPopup = true
            markers[driver.id].openPopup(markers[driver.id].getLatLng())
          }
        }
      })

      for (driverId in markers) {
        if (driversIds.indexOf(parseInt(driverId, 10)) < 0) {
          markers[driverId].setOpacity(0)
          //orders[orderId].removeFrom(map);
        }
      }
    })

    injectedSocket.on('orders_coordinates', function (data) {
      var orderLat,
        orderLon,
        marker,
        ordersIds = [],
        orderId,
        iconFileName,
        orderIcon
      //ctrl.list = data.drivers;
      //ctrl.userId = data.userId;

      data.orders.forEach(function (order) {
        orderLat = order.lat
        orderLon = order.lon
        ordersIds.push(parseInt(order.id, 10))

        if (orderLat && orderLon) {
          if (!orderIcons[order.id]) {
            orderIcons[order.id] = L.icon({
              // создаем иконку
              iconUrl: '/images/man.png',
              iconSize: [25, 25]
            })
          }

          iconFileName = order.vypolnyaetsya_voditelem > 0 ? 'man' : 'man_free'
          orderIcon = new L.DivIcon({
            className: 'my-div-icon',
            html:
              '<img class="imageMarker" src="/images/' +
              iconFileName +
              '.png"/><span class="markerText">' +
              order.addr +
              '</span>'
          })
          if (!orders[order.id]) {
            orders[order.id] = L.sm.marker([orderLat, orderLon], {
              //orderIcons[order.id], // передаем иконку маркеру
              icon: orderIcon,
              title: '' + order.addr
              //popup: '' + order.addr, // здесь может быть код HTML или DOM-элемент
              //popupOptions: {
              //	open: true,
              //	autoClose: false
              //}
            })
            orders[order.id].bindPopup('' + order.addr)
          } else {
            orders[order.id].setIcon(orderIcon)
            orders[order.id].setLatLng([orderLat, orderLon])
          }

          orders[order.id].addTo(map)
        }
      })

      for (orderId in orders) {
        if (ordersIds.indexOf(parseInt(orderId, 10)) < 0) {
          orders[orderId].setOpacity(0)
          //orders[orderId].removeFrom(map);
        }
      }
    })

    var getAppState = function () {
      injectedSocket.emit('app-state', {
        drivers: true,
        orders_coordinates: true
      })
    }

    setInterval(getAppState, 10000)

    /*this.state = ascInterface.state;
		//this.elAction = function() {
		//	ascInterface.state.call(ctrl, event);
		//};

		ctrl.updateHero = function(hero, prop, value) {
			hero[prop] = value;
		};

		ctrl.deleteHero = function(hero) {
			var idx = ctrl.list.indexOf(hero);
			if (idx >= 0) {
				ctrl.list.splice(idx, 1);
			}
		};*/
  }

  angular.module('gemStore').component('driversMap', {
    templateUrl: 'driversMap.html',
    controller: DriversMapController,
    bindings: {
      userId: '@?'
    }
  })
})(window.angular)
