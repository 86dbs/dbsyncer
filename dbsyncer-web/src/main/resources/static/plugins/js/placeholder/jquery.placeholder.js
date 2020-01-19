(function($){
    $.fn.PlaceHolder = function (opt) {
        var defaultValue = {
            zIndex: '0',
            top: '7px',
            left: '14px',
            fontSize: '14px',
            color: '#999'
        };

        return this.each(function () {
            var $self = $(this)
                , type = this.type
                , tagName = this.tagName.toLocaleLowerCase()
                , txt_placeholder = $(this).attr('placeholder') ? $(this).attr('placeholder') : ''
                , ie89 = document.documentMode == 8 || document.documentMode == 9
                , settings = $.extend({}, defaultValue, opt);

            if (!ie89 || type == 'hidden' || $self.attr('noPlaceholder') || (tagName != 'input' && tagName != 'textarea')) {
                return true;
            }

            var $container = $("<div class='placeholder-container'></div>");
            var $label = $('<label class="placeholder">' + txt_placeholder + '</label>');
            $container.css({
                display: 'inline-block',
                position: 'relative',
                width: $self.outerWidth(true) + 'px',
                height: $self.outerHeight(true) + 'px',
            });

            $label.css({
                position: 'absolute',
                zIndex: settings.zIndex,
                top: settings.top,
                left: settings.left,
                fontSize: settings.fontSize,
                color: settings.color ? settings.color : $self.css('color'),
                cursor: 'text',
                fontWeight: 'normal',
                display: 'none'
            });
            $label.on('click', function () {
                $self.trigger('focus');
            });

            if (!$self.parent().hasClass("placeholder-container")) {
                $self.wrap($container);
            } else {
                $label.text($self.attr('placeholder'));
            }

            if ($self.next('.placeholder').length != 0) {
                $self.next('.placeholder').remove()
            }

            if ($self.val() == '') {
                $label.show();
            }

            $self.parent().append($label);
            $self.off('.placeholder');
            $self.on('keydown.placeholder', function () {
                $self.next('.placeholder').hide();
            }).on('blur.placeholder', function () {
                if ($self.val() == '') {
                    $self.next('.placeholder').show();
                }
            }).on('change.placeholder', function () {
                if ($self.val() == '') {
                    $self.next('.placeholder').show();
                } else {
                    $self.next('.placeholder').hide();
                }
            })
        })
    };

})(jQuery);
