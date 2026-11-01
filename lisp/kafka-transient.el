;;; kafka-transient.el --- Transient menus for kafka.el  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 kafka.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Owned by the producer-transient lane.  Loaded last by kafka.el so it can
;; reference commands from every other module.

;;; Code:

(require 'kafka-rpc)

(provide 'kafka-transient)
;;; kafka-transient.el ends here
