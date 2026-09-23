define(['jquery', 'underscore', 'twigjs'], function ($, _, Twig) {
  var CustomWidget = function () {

    var self = this;
    var widgetRootClass = 'yandex-forms-widget';
    var styleLinkId = 'yandex-forms-widget-style';

    function isSettingsArea() {
      var area = self.system && self.system() ? String(self.system().area || '') : '';
      return area === 'settings' || area.indexOf('settings') === 0;
    }

    function escapeHtml(value) {
      return String(value === undefined || value === null ? '' : value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
    }

    function getLeadId() {
      var directCardId = self.system && self.system().amouser_id ? null : null;
      var path = (window.location.pathname || '') + ' ' + (window.location.hash || '');
      var match = path.match(/(?:leads\/detail\/|lead\/detail\/|lcard\/)(\d+)/);
      if (match) {
        return parseInt(match[1], 10) || 0;
      }

      if (window.AMOCRM && window.AMOCRM.data && window.AMOCRM.data.current_card && window.AMOCRM.data.current_card.id) {
        return parseInt(window.AMOCRM.data.current_card.id, 10) || 0;
      }

      if (window.APP && window.APP.data && window.APP.data.current_card && window.APP.data.current_card.id) {
        return parseInt(window.APP.data.current_card.id, 10) || 0;
      }

      return directCardId || 0;
    }

    function getSettings() {
      var settings = self.get_settings ? (self.get_settings() || {}) : {};
      var normalized = {
        serverUrl: settings.server_url || 'https://srm.chinatutor.ru/yandex_forms_api.php',
        leadParamName: settings.lead_param_name || 'amo_lead_id',
        portalParamName: settings.portal_param_name || 'amo_portal',
        widgetPath: settings.path || settings.cdn_path || '',
        version: settings.version || '1.0.3'
      };
      console.log('[yandex_forms_widget] settings', normalized, settings);
      return normalized;
    }

    function getPortalKey() {
      var hostname = String(window.location.hostname || '').toLowerCase();
      var match = hostname.match(/^([^.]+)\.amocrm\.ru$/);

      if (match && match[1]) {
        return match[1];
      }

      if (window.AMOCRM && window.AMOCRM.constant && window.AMOCRM.constant('account')) {
        var account = window.AMOCRM.constant('account');
        if (account && account.subdomain) {
          return String(account.subdomain).toLowerCase();
        }
      }

      return '';
    }

    function ensureStylesInjected() {
      var settings = getSettings();
      var path = settings.widgetPath || '';
      var href = path ? path + '/style.css?v=' + encodeURIComponent(settings.version || '1.0.3') : 'style.css';

      if (!document.getElementById(styleLinkId)) {
        $('<link>', {
          id: styleLinkId,
          rel: 'stylesheet',
          type: 'text/css',
          href: href
        }).appendTo('head');
        return;
      }

      $('#' + styleLinkId).attr('href', href);
    }

    function renderBase() {
      var html = ''
        + '<div class="' + widgetRootClass + '">'
        + '  <div class="' + widgetRootClass + '__body">'
        + '    <div class="' + widgetRootClass + '__controls" style="display:none;">'
        + '      <label class="' + widgetRootClass + '__label">Форма</label>'
        + '      <select class="' + widgetRootClass + '__select"></select>'
        + '      <div class="' + widgetRootClass + '__meta">'
        + '        <div class="' + widgetRootClass + '__meta-label">Публичная ссылка формы</div>'
        + '        <a class="' + widgetRootClass + '__meta-link" href="#" target="_blank" rel="noopener noreferrer"></a>'
        + '      </div>'
        + '      <label class="' + widgetRootClass + '__label">Ссылка</label>'
        + '      <div class="' + widgetRootClass + '__link-card">'
        + '        <textarea class="' + widgetRootClass + '__link" rows="4" readonly></textarea>'
        + '      </div>'
        + '      <div class="' + widgetRootClass + '__actions">'
        + '        <button class="button-input button-input_blue ' + widgetRootClass + '__copy">Скопировать ссылку</button>'
        + '      </div>'
        + '    </div>'
        + '  </div>'
        + '</div>';

      self.render_template({
        caption: {
          class_name: widgetRootClass + '__caption'
        },
        body: '',
        render: html
      });
    }

    function copyToClipboard(text, onDone) {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(function () {
          onDone(true);
        }).catch(function () {
          onDone(false);
        });
        return;
      }

      var $temp = $('<textarea>').css({
        position: 'fixed',
        top: '-1000px',
        left: '-1000px'
      }).val(text);

      $('body').append($temp);
      $temp[0].select();

      try {
        var success = document.execCommand('copy');
        onDone(success);
      } catch (e) {
        onDone(false);
      }

      $temp.remove();
    }

    function bindWidget(forms, setup) {
      var $root = $('.' + widgetRootClass);
      var $controls = $root.find('.' + widgetRootClass + '__controls');
      var $select = $root.find('.' + widgetRootClass + '__select');
      var $link = $root.find('.' + widgetRootClass + '__link');
      var $copy = $root.find('.' + widgetRootClass + '__copy');
      var $metaLink = $root.find('.' + widgetRootClass + '__meta-link');
      var copyLabelDefault = 'Скопировать ссылку';
      var copyResetTimer = null;

      $select.empty();

      forms.forEach(function (form, index) {
        var option = $('<option>')
          .val(form.generated_link)
          .text(form.name)
          .attr('data-public-url', form.public_url)
          .attr('data-form-id', form.id);

        if (index === 0) {
          option.attr('selected', 'selected');
        }

        $select.append(option);
      });

      function syncLink() {
        var $selected = $select.find('option:selected');
        var publicUrl = $selected.attr('data-public-url') || '';

        $link.val($select.val() || '');
        $metaLink.attr('href', publicUrl).text(publicUrl || 'Ссылка будет доступна после публикации формы.');
        $metaLink.toggleClass(widgetRootClass + '__meta-link_muted', !publicUrl);
      }

      function resetCopyButton() {
        if (copyResetTimer) {
          clearTimeout(copyResetTimer);
          copyResetTimer = null;
        }

        $copy.removeClass(widgetRootClass + '__copy_success ' + widgetRootClass + '__copy_fail');
        $copy.text(copyLabelDefault);
      }

      $select.off('change.' + widgetRootClass).on('change.' + widgetRootClass, syncLink);
      $copy.off('click.' + widgetRootClass).on('click.' + widgetRootClass, function (event) {
        event.preventDefault();
        var text = $link.val();
        if (!text) {
          return false;
        }

        copyToClipboard(text, function (success) {
          resetCopyButton();
          $copy.addClass(success ? widgetRootClass + '__copy_success' : widgetRootClass + '__copy_fail');
          $copy.text(success ? 'Скопировано' : 'Скопируйте вручную');
          copyResetTimer = setTimeout(resetCopyButton, 1800);
        });

        return false;
      });

      syncLink();
      $controls.show();
    }

    function loadForms() {
      var settings = getSettings();
      var leadId = getLeadId();
      var portalKey = getPortalKey();
      var requestData = {
        action: 'forms',
        lead_id: leadId,
        lead_param_name: settings.leadParamName
      };
      var $root = $('.' + widgetRootClass);

      if (settings.portalParamName && portalKey) {
        requestData[settings.portalParamName] = portalKey;
      }

      console.log('[yandex_forms_widget] loadForms start', {
        serverUrl: settings.serverUrl,
        leadParamName: settings.leadParamName,
        portalParamName: settings.portalParamName,
        leadId: leadId,
        portalKey: portalKey,
        area: self.system && self.system() ? self.system().area : null,
        location: window.location.href
      });

      if (!leadId) {
        console.warn('[yandex_forms_widget] lead_id not found');
        return;
      }

      console.log('[yandex_forms_widget] ajax request', {
        url: settings.serverUrl,
        data: requestData
      });

      $.ajax({
        url: settings.serverUrl,
        method: 'GET',
        dataType: 'json',
        data: requestData
      }).done(function (response) {
        console.log('[yandex_forms_widget] ajax success', response);
        if (!response || !response.success) {
          return;
        }

        if (!response.forms || !response.forms.length) {
          return;
        }

        bindWidget(response.forms, response.setup || {});
      }).fail(function (xhr) {
        console.error('[yandex_forms_widget] ajax fail', {
          status: xhr && xhr.status,
          responseText: xhr && xhr.responseText,
          responseJSON: xhr && xhr.responseJSON
        });
        var errorText = 'Ошибка загрузки форм.';
        if (xhr && xhr.responseJSON && xhr.responseJSON.error) {
          errorText = xhr.responseJSON.error;
        }
        console.error('[yandex_forms_widget] loadForms error text', errorText);
      });
    }

    this.callbacks = {
      render: function () {
        if (!isSettingsArea()) {
          console.log('[yandex_forms_widget] render lead card');
          ensureStylesInjected();
          renderBase();
        } else {
          console.log('[yandex_forms_widget] render settings area skipped');
        }
        return true;
      },

      init: function () {
        if (!isSettingsArea()) {
          console.log('[yandex_forms_widget] init lead card');
          setTimeout(loadForms, 100);
        } else {
          console.log('[yandex_forms_widget] init settings area');
        }
        return true;
      },

      bind_actions: function () {
        return true;
      },

      settings: function () {
        var $modalBody = $('.modal.' + self.get_settings().widget_code + ' .modal-body');
        if ($modalBody.length) {
          $modalBody.append(
            '<div style="margin-top:12px;font-size:13px;line-height:1.45;color:#555;">'
            + 'Серверный API должен быть доступен извне. В каждой Яндекс.Форме нужен скрытый вопрос с ID <b>amo_lead_id</b> и webhook на <b>/yandex_forms_webhook.php</b>.'
            + '</div>'
          );
        }
        return true;
      },

      onSave: function () {
        return true;
      },

      destroy: function () {
        $('.' + widgetRootClass + '__copy').off('.' + widgetRootClass);
        $('.' + widgetRootClass + '__select').off('.' + widgetRootClass);
        return true;
      }
    };

    return this;
  };

  return CustomWidget;
});
