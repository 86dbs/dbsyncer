/**
 * 前端功能权限：配合 data-permission 属性与 DBSPermission.has()。
 * 权限码与后端种子一致，如 connector:edit、mapping:operate。
 */
(function (window, $) {
    'use strict';

    const store = {
        admin: false,
        codes: {},
        loaded: false
    };

    function normalizeCodes(list) {
        const map = {};
        if (!list || !list.length) {
            return map;
        }
        for (let i = 0; i < list.length; i++) {
            const code = list[i];
            if (code) {
                map[String(code)] = true;
            }
        }
        return map;
    }

    const DBSPermission = {
        /**
         * @param {{admin?: boolean, permissions?: string[]}|null} data
         */
        load: function (data) {
            store.admin = !!(data && data.admin);
            store.codes = normalizeCodes(data && data.permissions);
            store.loaded = true;
        },

        isLoaded: function () {
            return store.loaded;
        },

        isAdmin: function () {
            return store.admin;
        },

        /**
         * 是否拥有权限（admin 视为全有）。
         * @param {string} code
         */
        has: function (code) {
            if (!code) {
                return true;
            }
            if (store.admin) {
                return true;
            }
            return !!store.codes[code];
        },

        /**
         * 多个权限码任一满足即可（逗号分隔）。
         * @param {string} codes
         */
        hasAny: function (codes) {
            if (!codes) {
                return true;
            }
            const parts = String(codes).split(',');
            for (let i = 0; i < parts.length; i++) {
                const c = $.trim(parts[i]);
                if (c && DBSPermission.has(c)) {
                    return true;
                }
            }
            return false;
        },

        /**
         * 按 data-permission 显隐；无权限加 hidden。
         * @param {string|HTMLElement|jQuery} [root]
         */
        apply: function (root) {
            const $root = root ? $(root) : $(document);
            $root.find('[data-permission]').addBack('[data-permission]').each(function () {
                const $el = $(this);
                const need = $el.attr('data-permission');
                if (DBSPermission.hasAny(need)) {
                    $el.removeClass('hidden');
                } else {
                    $el.addClass('hidden');
                }
            });
        }
    };

    window.DBSPermission = DBSPermission;
})(window, jQuery);
