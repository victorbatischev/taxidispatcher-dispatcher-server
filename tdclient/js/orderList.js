;(function (angular) {
  'use strict'
  function OrderListController(
    $scope,
    $element,
    $attrs,
    injectedSocket,
    ascInterface
  ) {
    var ctrl = this,
      routeCoords = [null, null]

    ctrl.list = []
    ctrl.userId = -1
    ctrl.depends = {}
    injectedSocket.on('orders', function (data) {
      ctrl.list = data.orders
      ctrl.userId = data.userId
      ctrl.depends = data.depends || {}
      ctrl.depends.yesno = [
        { BOLD_ID: 1, Naimenovanie: 'ДА' },
        { BOLD_ID: 0, Naimenovanie: 'НЕТ' }
      ]
    })

    var refreshOrdersCallback = function () {
      $('.fixed-state-informer').addClass('hide-text')
    }

    this.refreshOrdersCallback = refreshOrdersCallback

    injectedSocket.on('is_order_data_updates', function (data) {
      console.log('is_order_data_updates')
      $('.fixed-state-informer').removeClass('hide-text')
    })

    injectedSocket.on('abort_connection', function (data) {
      alert('Соединение закрыто! Сообщение: ' + data.msg)
    })

    this.showMapOrderAddForm = function () {
      $('#map-add-form').toggleClass('hidden_form')
    }

    this.findCoordByAdr = function (e, id) {
      injectedSocket.emit('get-addr-coords', {
        mode: id,
        address: $('#' + id).val()
      })
    }

    injectedSocket.on('detected-addr-coords', function (data) {
      $('#' + data.mode + '__coords').html(
        'lat: ' + data.lat + ', lon: ' + data.lon
      )
      var routeCoordsIdx = data.mode.indexOf('start-adr') >= 0 ? 0 : 1,
        secCoordIdx = routeCoordsIdx ? 0 : 1
      routeCoords[routeCoordsIdx] = {
        lat: data.lat,
        lon: data.lon
      }
      if (routeCoords[secCoordIdx] !== null) {
        injectedSocket.emit('get-route', routeCoords)
      }
    })

    injectedSocket.on('get-route-result', function (data) {
      //alert(JSON.stringify(data));
      if (!data || !data.route_summary) {
        $('#route-build-result').text('Не найдено маршрута!')
      } else {
        var routeSummary = data.route_summary
        $('#route-build-result').text(
          'Расстояние: ' +
            (routeSummary.total_distance / 1000).toFixed(2) +
            ' км., ' +
            'время: ' +
            (routeSummary.total_time / 60).toFixed(2) +
            'мин.'
        )
        $('#new-order-from-route-button').removeClass('hidden_button')
      }

      if (
        data &&
        data.custom_options &&
        data.custom_options.data &&
        data.custom_options.data[0].sector_id &&
        data.custom_options.data[1].sector_id
      ) {
        var sectorsData = data.custom_options.data
        $('#sectors-route-result').text(
          'Начальный сектор: ' +
            sectorsData[0].sector_name +
            ', конечный сектор: ' +
            sectorsData[1].sector_name
        )
        $('#new-order-from-route-button').removeClass('hidden_button')
      } else {
        $('#sectors-route-result').text(
          'Не найдено попадания обеих точек в сектора!'
        )
      }
    })

    this.findCoordOnMap = function (e, id) {}

    this.state = ascInterface.state
    //this.elAction = function() {
    //	ascInterface.state.call(ctrl, event);
    //};

    ctrl.updateHero = function (hero, prop, value) {
      hero[prop] = value
    }

    ctrl.deleteHero = function (hero) {
      var idx = ctrl.list.indexOf(hero)
      if (idx >= 0) {
        ctrl.list.splice(idx, 1)
      }
    }
  }

  angular
    .module('gemStore')
    .directive('orderTabs', function () {
      return {
        restrict: 'E',
        templateUrl: 'orderTabs.html',
        controller: function () {
          this.tab = 1

          this.isSet = function (checkTab) {
            return this.tab === checkTab
          }

          this.setTab = function (activeTab) {
            this.tab = activeTab
          }
        },
        controllerAs: 'tab'
      }
    })
    .directive('orderDescription', function () {
      return {
        restrict: 'E',
        templateUrl: 'orderDescription.html'
      }
    })
    .directive('orderSpecs', function () {
      return {
        restrict: 'A',
        templateUrl: 'orderSpecs.html'
      }
    })
    .component('orderList', {
      templateUrl: 'orderList.html',
      controller: OrderListController,
      bindings: {
        userId: '@?'
      }
    })
})(window.angular)
