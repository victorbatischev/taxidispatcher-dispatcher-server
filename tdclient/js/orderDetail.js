;(function (angular) {
  'use strict'
  function OrderDetailController($scope, $element, $attrs, ascInterface) {
    var ctrl = this

    this.state = ascInterface.state

    ctrl.update = function (prop, value) {
      ctrl.onUpdate({ hero: ctrl.hero, prop: prop, value: value })
    }

    ctrl.updateList = function (props) {
      for (var i in props) {
        ctrl.hero[i] = props[i]
      }
      //ctrl.onUpdate({hero: ctrl.hero, prop: prop, value: value});
    }
  }

  angular.module('gemStore').component('orderDetail', {
    templateUrl: 'orderDetail.html',
    controller: OrderDetailController,
    bindings: {
      hero: '<',
      depends: '<',
      onDelete: '&',
      onUpdate: '&'
    }
  })
})(window.angular)
