;(function (angular) {
  'use strict'
  function EditFieldController($scope, $element, $attrs, ascInterface) {
    var ctrl = this
    ctrl.editMode = false
    //console.log('dateValue: ' + ctrl.fieldValue);
    //$scope.dateValue = new Date(ctrl.fieldValue);

    ctrl.keyPress = function (event, isTouch) {
      if ((event.which || event.keyCode) === 13 || isTouch) {
        if (ctrl.editMode) {
          ctrl.handleModeChange()
          //ctrl.
          ascInterface.state.call(ctrl, event)
        }
      }
    }

    ctrl.selectChange = function (event) {
      if (ctrl.editMode) {
        //ctrl.fieldHideValue = $(this).val();
        //ctrl.handleModeChange();
        ctrl.handleModeChangeSelect(event)
        ascInterface.state.call(ctrl, event)
      }
    }

    ctrl.dateChange = function (event) {
      if (ctrl.editMode) {
        //ctrl.fieldHideValue = $(this).val();
        //ctrl.handleModeChange();
        ctrl.handleModeChangeDate(event)
        ascInterface.state.call(ctrl, event)
      }
    }

    ctrl.checkboxChange = function (event) {
      //ctrl.fieldHideValue = $(this).val();
      //ctrl.handleModeChange();
      ctrl.handleModeChangeCheckbox(event)
      ascInterface.state.call(ctrl, event)
    }

    ctrl.handleModeChange = function (e) {
      e && e.preventDefault()
      if (ctrl.editMode) {
        ctrl.onUpdate({ value: ctrl.fieldValue })
        ctrl.fieldValueCopy = ctrl.fieldValue
      } else {
        //$scope.dateValue = new Date(ctrl.fieldValue);
      }
      //console.log('dateValue: ' + ctrl.fieldValue);
      ctrl.editMode = !ctrl.editMode
      var $editField = e && $(e.target).closest('.editField')

      if (ctrl.editMode && $editField) {
        var setTextInputFocus = function (editField) {
            $(editField).find('input[type=text]').focus()
          },
          bindFocusHandler = function () {
            return setTextInputFocus.apply(this, $editField)
          }
        setTimeout(bindFocusHandler, 500)
        if (ctrl.fieldType.indexOf('datetime') == 0) {
          var assingDateTimePickerPlugin = function (editField) {
              $(editField).find('input.datetimepicker').datetimepicker()
            },
            bindDTPicker = function () {
              return assingDateTimePickerPlugin.apply(this, $editField)
            }
          setTimeout(bindDTPicker, 500)
        }
      }
    }

    ctrl.handleModeChangeSelect = function (e) {
      e && e.preventDefault()
      if (ctrl.editMode) {
        //ctrl.onUpdate2({value: ctrl.fieldValue});
        ctrl.onUpdate({ value: ctrl.fieldHideValue })
        ctrl.fieldValueCopy = ctrl.fieldValue
      }
      ctrl.editMode = !ctrl.editMode
    }

    ctrl.handleModeChangeCheckbox = function (e) {
      e && e.preventDefault()
      ctrl.onUpdate({ value: ctrl.fieldValue ? 1 : 0 })
      ctrl.fieldValueCopy = ctrl.fieldValue
    }

    ctrl.handleModeChangeDate = function (e) {
      e && e.preventDefault()
      if (ctrl.editMode) {
        //ctrl.onUpdate2({value: ctrl.fieldValue});
        //ctrl.fieldValue = $scope.dateValue.toLocaleString();
        //console.log($scope.dateValue.toLocaleString());
        ctrl.onUpdate({ value: ctrl.fieldValue })
        ctrl.fieldValueCopy = ctrl.fieldValue
      }
      ctrl.editMode = !ctrl.editMode
    }

    ctrl.reset = function () {
      ctrl.fieldValue = ctrl.fieldValueCopy
    }

    ctrl.$onInit = function () {
      // Make a copy of the initial value to be able to reset it later
      ctrl.fieldValueCopy = ctrl.fieldValue

      // Set a default fieldType
      if (!ctrl.fieldType) {
        ctrl.fieldType = 'text'
      }
    }
  }

  angular.module('gemStore').component('editField', {
    templateUrl: 'editField.html',
    controller: EditFieldController,
    bindings: {
      fieldValue: '<',
      fieldPlaceholder: '<',
      fieldLabel: '<',
      fieldStyles: '<',
      fieldType: '@?',
      fieldName: '@?',
      fieldHideName: '@?',
      fieldHideValue: '<',
      fieldId: '@?',
      disabled: '<',
      depends: '<?',
      onUpdate: '&'
    }
  })
})(window.angular)

/*
Copyright 2018 Google Inc. All Rights Reserved.
Use of this source code is governed by an MIT-style license that
can be found in the LICENSE file at http://angular.io/license
*/
