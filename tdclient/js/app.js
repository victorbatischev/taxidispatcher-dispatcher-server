;(function () {
  var parser = document.createElement('a')
  parser.href = window.location

  //console.log(window.location);
  //console.log(parser.protocol + '//' + parser.hostname + ':' + parser.port + '/');

  var socketUrl =
      parser.protocol + '//' + parser.hostname + ':' + parser.port + '/',
    app = angular
      .module('gemStore', ['btford.socket-io', 'store-directives'])
      .factory('injectedSocket', function (socketFactory) {
        return socketFactory({
          prefix: '',
          ioSocket: io.connect(socketUrl)
        })
      })
      .factory('ascInterface', function (injectedSocket) {
        var factory = {}

        factory.state = function (e, callback) {
          e && e.preventDefault()
          var $target = $(event.target).hasClass('state')
              ? $(event.target)
              : $(event.target).closest('.editScope').find('.state'),
            $aspElement = $target.data('aspect0'),
            $injectedAspectClass = $target.data('injected-aspect0'),
            $injectedAspect =
              $injectedAspectClass &&
              $('.aspect_inject_' + $injectedAspectClass),
            $ctxElement = $target.closest('div[data-ctx0]'),
            $injectedCtxElm =
              $injectedAspect && $injectedAspect.closest('div[data-ctx0]')
          if ($target.hasClass('state') && $ctxElement.length) {
            var stateParams = {},
              i = 0,
              isCtxParam = false,
              paramVal,
              $editField = $target.closest('.editField')
            while (
              !(typeof $target.data('state' + i + '-val') === 'undefined')
            ) {
              paramVal = $target.data('state' + i + '-val')
              isCtxParam =
                paramVal && paramVal.indexOf && paramVal.indexOf('ctx:') == 0
              stateParams[$target.data('state' + i) || 'state' + i] = isCtxParam
                ? getCtxParamVal($ctxElement, paramVal)
                : paramVal
              i++
            }

            i = 0
            while (
              $editField &&
              !(typeof $editField.data('state' + i + '-val') === 'undefined')
            ) {
              paramVal = $editField.data('state' + i + '-val')
              isCtxParam =
                paramVal && paramVal.indexOf && paramVal.indexOf('ctx:') == 0
              stateParams[$editField.data('state' + i) || 'state' + i] =
                isCtxParam ? getCtxParamVal($ctxElement, paramVal) : paramVal
              i++
            }

            if ($injectedAspect) {
              i = 0
              while (
                !(
                  typeof $injectedAspect.data('state' + i + '-val') ===
                  'undefined'
                )
              ) {
                paramVal = $injectedAspect.data('state' + i + '-val')
                isCtxParam =
                  paramVal && paramVal.indexOf && paramVal.indexOf('ctx:') == 0
                stateParams[$injectedAspect.data('state' + i) || 'state' + i] =
                  isCtxParam
                    ? getCtxParamVal($injectedCtxElm, paramVal)
                    : paramVal
                i++
              }
            }

            var actionData = $aspElement
              ? {
                  aspects: [$aspElement],
                  states: stateParams
                }
              : stateParams

            //console.log($ctxElement.data('ctx0'));
            injectedSocket.emit($ctxElement.data('ctx0'), actionData)
            callback && callback()
            //$injectedCtxElm && injectedSocket.emit($injectedCtxElm.data('ctx0'), actionData);
            //for (int i = 1; i < 10; i++)
          }

          function getCtxParamVal($ctxElm, paramName) {
            var i = 0,
              ctxParamName
            while ((ctxParamName = $ctxElm.data('state' + i))) {
              if (paramName === ctxParamName) {
                return $ctxElm.data('state' + i + '-val')
              }
              i++
            }

            var $closestCtxElm = $ctxElm.parent().closest('div[data-ctx0]')
            if ($closestCtxElm.length) {
              return getCtxParamVal($closestCtxElm, paramName)
            }

            return ''
          }
        }

        return factory
      })

  app.controller('IndexController', [
    '$http',
    'injectedSocket',
    function ($http, injectedSocket) {
      var store = this
      store.products = []

      this.authSubmit = function (product) {
        var $target = $(event.target),
          authParams = {}

        ;['login', 'psw', 'code'].forEach(function callback(
          currentValue,
          index,
          array
        ) {
          authParams[currentValue] = getAuthParam(currentValue)
        })
        injectedSocket.emit('ident', JSON.stringify(authParams))
        $target.closest('div').fadeOut()

        function getAuthParam(paramName) {
          return $target
            .closest('div')
            .find('input[id=' + paramName + ']')
            .val()
        }
      }

      injectedSocket.on('news', function (data) {
        //console.log('dddd' + JSON.stringify(data));
        //store.products = data.products;
      })
    }
  ])

  app.controller('ReviewController', function (injectedSocket) {
    injectedSocket.on('news', function (data) {
      //$scope.bar = true;
      //console.log('ggggg' + JSON.stringify(data));
    })

    this.review = {}

    this.addReview = function (product) {
      product.reviews.push(this.review)

      this.review = {}
    }
  })
})()
