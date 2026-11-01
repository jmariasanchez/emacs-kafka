;;; kafka-groups.el --- Consumer groups and lag  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 kafka.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Lists consumer groups and shows per-partition committed offsets and lag.
;; Describing a group is read-only: the bridge fetches committed offsets
;; without joining the group.

;;; Code:

(require 'kafka-rpc)
(require 'tabulated-list)
(require 'kafka-acls)

(defvar kafka-connection)
(declare-function kafka-ensure-connected "kafka")
(declare-function kafka-read-connection "kafka")
(declare-function kafka-header-line "kafka")
(declare-function kafka-confirm-exact "kafka")
(declare-function kafka-confirm-write "kafka")
(declare-function kafka-records "kafka-records")

(defcustom kafka-groups-hide-inspection t
  "When non-nil, hide kafka.el's own transient inspection groups."
  :type 'boolean :group 'kafka)

(defvar-keymap kafka-groups-mode-map
  :parent tabulated-list-mode-map
  "RET" #'kafka-groups-describe
  "D" #'kafka-groups-delete
  "R" #'kafka-group-reset-offsets
  "m" #'kafka-group-members
  "g" #'revert-buffer)

(define-derived-mode kafka-groups-mode tabulated-list-mode "Kafka Groups"
  "Major mode listing consumer groups."
  (setq tabulated-list-format [("Group" 50 t) ("State" 14 t) ("Type" 10 t)
                               ("Protocol" 12 t) ("Members" 8 nil :right-align t)])
  (setq tabulated-list-padding 1)
  (setq tabulated-list-sort-key '("Group"))
  (add-hook 'tabulated-list-revert-hook #'kafka-groups--refresh nil t)
  (tabulated-list-init-header))

(defun kafka-groups--refresh ()
  (kafka-ensure-connected kafka-connection)
  (let ((gs (seq-remove (lambda (g)
                          (and kafka-groups-hide-inspection
                               (string-prefix-p "kafka.el-inspect" (plist-get g :groupId))))
                        (kafka-rpc-request :kafka/groups/list (list :connectionId kafka-connection)))))
    (setq header-line-format (kafka-header-line kafka-connection (format "%d groups" (length gs))))
    (setq tabulated-list-entries
          (mapcar (lambda (g)
                    (list (plist-get g :groupId)
                          (vector (plist-get g :groupId)
                                  (let ((s (plist-get g :state)))
                                    (propertize s 'face (pcase s ("Stable" 'success) ("Dead" 'shadow)
                                                          ("Empty" 'shadow) (_ 'warning))))
                                  (plist-get g :protocolType)
                                  (plist-get g :protocol)
                                  (number-to-string (plist-get g :members)))))
                  gs))))

;;;###autoload
(defun kafka-groups (name)
  "List consumer groups on connection NAME."
  (interactive (list (kafka-read-connection)))
  (kafka-ensure-connected name)
  (with-current-buffer (get-buffer-create (format "*Kafka Groups: %s*" name))
    (kafka-groups-mode)
    (setq kafka-connection name)
    (revert-buffer)
    (pop-to-buffer (current-buffer))))

(defun kafka-groups-delete ()
  "Delete the group at point.  The group must have no active members."
  (interactive)
  (let* ((g (or (tabulated-list-get-id) (user-error "No group here")))
         (confirm (kafka-confirm-exact kafka-connection
                                       (format "DELETE consumer group %s and its committed offsets" g) g)))
    (kafka-rpc-request :kafka/group/delete
                       (list :connectionId kafka-connection :groupId g :confirmName confirm))
    (message "Deleted group %s" g)
    (revert-buffer)))

;;;; Group detail

(defvar-local kafka-group--id nil)

(defvar-keymap kafka-group-mode-map
  :parent tabulated-list-mode-map
  "RET" #'kafka-group-records-at-committed
  "R" #'kafka-group-reset-offsets
  "m" #'kafka-group-members
  "g" #'revert-buffer)

(define-derived-mode kafka-group-mode tabulated-list-mode "Kafka Group"
  "Major mode showing a consumer group's offsets and lag."
  (setq tabulated-list-format [("Topic" 40 t) ("P" 4 nil :right-align t)
                               ("Committed" 12 nil :right-align t)
                               ("End" 12 nil :right-align t)
                               ("Lag" 10 kafka-group--sort-lag :right-align t)])
  (setq tabulated-list-padding 1)
  (add-hook 'tabulated-list-revert-hook #'kafka-group--refresh nil t)
  (tabulated-list-init-header))

(defun kafka-group--sort-lag (a b)
  (< (or (plist-get (car a) :lag) 0) (or (plist-get (car b) :lag) 0)))

(defun kafka-group--refresh ()
  (let ((d (kafka-rpc-request :kafka/group/describe
                              (list :connectionId kafka-connection :groupId kafka-group--id))))
    (setq header-line-format
          (kafka-header-line kafka-connection
                             (propertize kafka-group--id 'face 'font-lock-constant-face)
                             (plist-get d :state)
                             (format "%d members" (plist-get d :members))
                             (format "total lag %d" (plist-get d :totalLag))))
    (setq tabulated-list-entries
          (mapcar (lambda (o)
                    (let ((lag (plist-get o :lag)))
                      (list o (vector (plist-get o :topic)
                                      (number-to-string (plist-get o :partition))
                                      (if (plist-get o :committed) (number-to-string (plist-get o :committed)) "-")
                                      (number-to-string (plist-get o :high))
                                      (propertize (if lag (number-to-string lag) "-")
                                                  'face (if (and lag (> lag 0)) 'warning 'default))))))
                  (plist-get d :offsets)))))

(defun kafka-groups-describe ()
  "Describe the group at point."
  (interactive)
  (kafka-group kafka-connection (or (tabulated-list-get-id) (user-error "No group here"))))

(defun kafka-group (name group)
  "Describe consumer GROUP on NAME."
  (interactive (list (kafka-read-connection) (read-string "Group: ")))
  (kafka-ensure-connected name)
  (with-current-buffer (get-buffer-create (format "*Kafka Group: %s/%s*" name group))
    (kafka-group-mode)
    (setq kafka-connection name kafka-group--id group)
    (revert-buffer)
    (pop-to-buffer (current-buffer))))

(defun kafka-group-records-at-committed ()
  "Browse the partition at point starting at the group's committed offset.
This reads independently and does not affect the group."
  (interactive)
  (let ((o (or (tabulated-list-get-id) (user-error "No partition here"))))
    (kafka-records kafka-connection (plist-get o :topic) (list (plist-get o :partition))
                   (list :type "offset" :offset (or (plist-get o :committed) 0)))))

(defun kafka-group--selected-id ()
  "Return the group id in the current group view, or prompt."
  (or kafka-group--id
      (and (derived-mode-p 'kafka-groups-mode) (tabulated-list-get-id))
      (read-string "Group: ")))

(defun kafka-group-members (name)
  "Show members, assignments and coordinator on connection NAME."
  (interactive (list (kafka-read-connection)))
  (let ((group (kafka-group--selected-id)))
    (kafka-ensure-connected name)
    (kafka-admin--show name (format "Members %s" group)
                       (kafka-rpc-request :kafka/group/members (list :connectionId name :groupId group)))))

(defvar-local kafka-group-reset--params nil)
(defvar-keymap kafka-group-reset-mode-map
  :parent tabulated-list-mode-map
  "C-c C-c" #'kafka-group-reset-apply)
(define-derived-mode kafka-group-reset-mode tabulated-list-mode "Kafka Reset Preview"
  "Preview offsets before resetting.  C-c C-c asks for the exact group id."
  (setq tabulated-list-format [("Topic" 40 t) ("P" 5 t) ("Current" 15 t) ("New offset" 15 t)])
  (tabulated-list-init-header))

(defun kafka-group-reset--selection (input)
  "Parse INPUT as topic or topic:partition,partition."
  (let* ((parts (split-string input ":"))
         (topic (car parts))
         (ids (and (cadr parts) (split-string (cadr parts) ","))))
    (unless (and (not (string-empty-p topic)) (<= (length parts) 2)
                 (or (not ids) (seq-every-p (lambda (s) (string-match-p "\\`[0-9]+\\'" s)) ids)))
      (user-error "Use topic or topic:0,1"))
    (if ids (list :topic topic :partitions (vconcat (mapcar #'string-to-number ids)))
      (list :topic topic))))

;;;###autoload
(defun kafka-group-reset-offsets (name)
  "Preview a reset of an inactive group's offsets on NAME.
No write is made until C-c C-c in the preview and an exact group confirmation."
  (interactive (list (kafka-read-connection)))
  (kafka-ensure-connected name)
  (kafka-confirm-write name "prepare an offset reset")
  (let* ((group (kafka-group--selected-id))
         (selection (kafka-group-reset--selection (read-string "Topic or topic:0,1: ")))
         (kind (completing-read "Reset: " '("to-earliest" "to-latest" "to-offset" "to-datetime" "shift-by") nil t))
         (spec (append (list :type kind)
                       (cond ((member kind '("to-offset" "shift-by")) (list :offset (read-number "Offset / shift: ")))
                             ((equal kind "to-datetime") (list :timestamp (read-number "Timestamp (epoch ms): "))))))
         (params (list :connectionId name :groupId group :topics (vector selection) :spec spec :dryRun t))
         (preview (kafka-rpc-request :kafka/group/resetOffsets params)))
    (with-current-buffer (get-buffer-create (format "*Kafka Reset: %s/%s*" name group))
      (kafka-group-reset-mode)
      (setq kafka-connection name kafka-group-reset--params params)
      (setq header-line-format (kafka-header-line name group "PREVIEW ONLY | C-c C-c to confirm | stop consumers before applying"))
      (setq tabulated-list-entries
            (mapcar (lambda (o)
                      (list o (vector (plist-get o :topic) (number-to-string (plist-get o :partition))
                                      (format "%s" (or (plist-get o :currentCommitted) "-"))
                                      (number-to-string (plist-get o :newOffset)))))
                    (plist-get preview :offsets)))
      (tabulated-list-print t)
      (pop-to-buffer (current-buffer)))))

(defun kafka-group-reset-apply ()
  "Confirm and apply the reset shown in this preview buffer."
  (interactive)
  (unless kafka-group-reset--params (user-error "No pending reset"))
  (let* ((params (copy-tree kafka-group-reset--params))
         (group (plist-get params :groupId))
         (confirm (kafka-confirm-exact kafka-connection
                                       (format "RESET offsets for %s (offsets are recalculated at execution)" group) group)))
    (setq params (plist-put params :dryRun :json-false))
    (setq params (plist-put params :confirmName confirm))
    (kafka-rpc-request :kafka/group/resetOffsets params)
    (setq kafka-group-reset--params nil)
    (setq header-line-format (kafka-header-line kafka-connection group "RESET APPLIED"))
    (message "Reset offsets for %s" group)))

(provide 'kafka-groups)
;;; kafka-groups.el ends here
