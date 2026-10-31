;;; kafka-topics.el --- Topic explorer and administration  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 kafka.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; `kafka-topics-mode' lists topics; `kafka-topic-mode' describes one topic
;; (partitions, leaders, ISR, watermarks); `kafka-config-mode' shows topic or
;; broker configuration.

;;; Code:

(require 'kafka-rpc)
(require 'tabulated-list)

(defvar kafka-connection)
(declare-function kafka-ensure-connected "kafka")
(declare-function kafka-read-connection "kafka")
(declare-function kafka-header-line "kafka")
(declare-function kafka-confirm-write "kafka")
(declare-function kafka-confirm-exact "kafka")
(declare-function kafka-json-bool "kafka")
(declare-function kafka-records "kafka-records")
(declare-function kafka-produce "kafka-producer")

(defcustom kafka-topics-show-internal nil
  "When non-nil, list internal topics such as __consumer_offsets."
  :type 'boolean
  :group 'kafka)

(defvar-local kafka-topics--filter nil)

(defvar-keymap kafka-topics-mode-map
  :parent tabulated-list-mode-map
  "RET" #'kafka-topics-describe
  "r" #'kafka-topics-records
  "c" #'kafka-topics-records
  "p" #'kafka-topics-produce
  "C" #'kafka-topics-configs
  "+" #'kafka-topic-create
  "D" #'kafka-topics-delete
  "/" #'kafka-topics-filter
  "i" #'kafka-topics-toggle-internal
  "g" #'revert-buffer)

(define-derived-mode kafka-topics-mode tabulated-list-mode "Kafka Topics"
  "Major mode listing Kafka topics.
\\{kafka-topics-mode-map}"
  (setq tabulated-list-format [("Topic" 50 t)
                               ("Partitions" 10 kafka-topics--sort-partitions :right-align t)
                               ("RF" 4 nil :right-align t)
                               ("URP" 4 nil :right-align t)
                               ("" 0 nil)])
  (setq tabulated-list-padding 1)
  (setq tabulated-list-sort-key '("Topic"))
  (add-hook 'tabulated-list-revert-hook #'kafka-topics--refresh nil t)
  (tabulated-list-init-header))

(defun kafka-topics--sort-partitions (a b)
  (< (string-to-number (aref (cadr a) 1)) (string-to-number (aref (cadr b) 1))))

(defun kafka-topics--entries (topics)
  "Convert TOPICS (vector of plists) to tabulated entries."
  (mapcar (lambda (tp)
            (let ((urp (plist-get tp :underReplicated)))
              (list (plist-get tp :name)
                    (vector (propertize (plist-get tp :name) 'face
                                        (if (kafka-json-bool (plist-get tp :internal)) 'shadow 'default))
                            (number-to-string (plist-get tp :partitions))
                            (number-to-string (plist-get tp :replicationFactor))
                            (if (> urp 0) (propertize (number-to-string urp) 'face 'error) "0")
                            (or (plist-get tp :error) "")))))
          topics))

(defun kafka-topics--refresh ()
  (kafka-ensure-connected kafka-connection)
  (let ((topics (kafka-rpc-request :kafka/topics/list
                                   (list :connectionId kafka-connection
                                         :includeInternal (if kafka-topics-show-internal t :json-false)
                                         :filter kafka-topics--filter))))
    (setq header-line-format
          (kafka-header-line kafka-connection
                             (format "%d topics" (length topics))
                             (if kafka-topics--filter (format "filter: %s" kafka-topics--filter) "")))
    (setq tabulated-list-entries (kafka-topics--entries topics))))

;;;###autoload
(defun kafka-topics (name)
  "List topics on connection NAME."
  (interactive (list (kafka-read-connection)))
  (kafka-ensure-connected name)
  (with-current-buffer (get-buffer-create (format "*Kafka Topics: %s*" name))
    (unless (derived-mode-p 'kafka-topics-mode) (kafka-topics-mode))
    (setq kafka-connection name)
    (revert-buffer)
    (pop-to-buffer (current-buffer))))

(defun kafka-topics--at-point ()
  (or (tabulated-list-get-id) (user-error "No topic on this line")))

(defun kafka-topics-filter (s)
  "Filter topics by substring S (empty clears)."
  (interactive (list (read-string "Filter topics: " kafka-topics--filter)))
  (setq kafka-topics--filter (if (string-empty-p s) nil s))
  (revert-buffer))

(defun kafka-topics-toggle-internal ()
  "Toggle listing of internal topics."
  (interactive)
  (setq-local kafka-topics-show-internal (not kafka-topics-show-internal))
  (revert-buffer))

(defun kafka-topics-records ()
  "Browse records of the topic at point."
  (interactive)
  (kafka-records kafka-connection (kafka-topics--at-point)))

(defun kafka-topics-produce ()
  "Compose a record for the topic at point."
  (interactive)
  (kafka-produce kafka-connection (kafka-topics--at-point)))

(defun kafka-topics-configs ()
  "Show configs of the topic at point."
  (interactive)
  (kafka-config-show kafka-connection 'topic (kafka-topics--at-point)))

(defun kafka--read-configs ()
  "Read KEY=VALUE pairs; return a plist suitable for JSON."
  (let ((s (read-string "Configs (key=value, comma separated, empty for none): "))
        out)
    (dolist (kv (split-string s "," t "[ \t]+") (nreverse out))
      (pcase (split-string kv "=")
        (`(,k ,v) (push (intern (concat ":" k)) out) (push v out))
        (_ (user-error "Bad config entry %S" kv))))))

(defun kafka-topic-create (name topic partitions rf configs)
  "Create TOPIC on NAME with PARTITIONS, replication RF and CONFIGS."
  (interactive
   (list (kafka-read-connection)
         (read-string "New topic name: ")
         (read-number "Partitions: " 1)
         (read-number "Replication factor: " 1)
         (kafka--read-configs)))
  (kafka-confirm-write name (format "Create topic %s (%d partitions, RF %d)" topic partitions rf))
  ;; Validate first so errors are reported before any change.
  (kafka-rpc-request :kafka/topic/create
                     (list :connectionId name :topic topic :partitions partitions
                           :replicationFactor rf :configs configs
                           :validateOnly t))
  (kafka-rpc-request :kafka/topic/create
                     (list :connectionId name :topic topic :partitions partitions
                           :replicationFactor rf :configs configs))
  (message "Created topic %s" topic)
  (when (derived-mode-p 'kafka-topics-mode) (revert-buffer)))

(defun kafka-topics-delete ()
  "Delete the topic at point after exact-name confirmation."
  (interactive)
  (let ((topic (kafka-topics--at-point)))
    (kafka-topic-delete kafka-connection topic)
    (revert-buffer)))

(defun kafka-topic-delete (name topic)
  "Delete TOPIC on NAME.  Requires typing the exact topic name."
  (interactive (list (kafka-read-connection) (read-string "Topic to delete: ")))
  (let ((confirm (kafka-confirm-exact name (format "DELETE topic %s and all its data" topic) topic)))
    (kafka-rpc-request :kafka/topic/delete
                       (list :connectionId name :topic topic :confirmName confirm))
    (message "Deleted topic %s" topic)))

;;;; Single topic

(defvar-local kafka-topic--name nil)
(defvar-local kafka-topic--detail nil)

(defvar-keymap kafka-topic-mode-map
  :parent tabulated-list-mode-map
  "RET" #'kafka-topic-records-partition
  "r" #'kafka-topic-records
  "p" #'kafka-topic-produce
  "C" #'kafka-topic-configs
  "+" #'kafka-topic-add-partitions
  "g" #'revert-buffer)

(define-derived-mode kafka-topic-mode tabulated-list-mode "Kafka Topic"
  "Major mode describing one topic's partitions."
  (setq tabulated-list-format [("P" 5 kafka--num-sort :right-align t)
                               ("Leader" 7 nil :right-align t)
                               ("Replicas" 14 nil) ("ISR" 14 nil)
                               ("Low" 12 nil :right-align t) ("High" 12 nil :right-align t)
                               ("Count" 12 nil :right-align t) ("" 0 nil)])
  (setq tabulated-list-padding 1)
  (add-hook 'tabulated-list-revert-hook #'kafka-topic--refresh nil t)
  (tabulated-list-init-header))

(declare-function kafka--num-sort "kafka")

(defun kafka-topic--ids (v)
  (mapconcat #'number-to-string v ","))

(defun kafka-topic--refresh ()
  (let ((d (kafka-rpc-request :kafka/topic/describe
                              (list :connectionId kafka-connection :topic kafka-topic--name))))
    (setq kafka-topic--detail d)
    (setq header-line-format
          (kafka-header-line kafka-connection (propertize kafka-topic--name 'face 'font-lock-constant-face)
                             (format "%d partitions" (length (plist-get d :partitions)))
                             (format "%d records" (plist-get d :totalMessages))))
    (setq tabulated-list-entries
          (mapcar (lambda (p)
                    (let ((under (< (length (plist-get p :isr)) (length (plist-get p :replicas)))))
                      (list (plist-get p :id)
                            (vector (number-to-string (plist-get p :id))
                                    (if (< (plist-get p :leader) 0)
                                        (propertize "none" 'face 'error)
                                      (number-to-string (plist-get p :leader)))
                                    (kafka-topic--ids (plist-get p :replicas))
                                    (propertize (kafka-topic--ids (plist-get p :isr))
                                                'face (if under 'error 'default))
                                    (number-to-string (plist-get p :low))
                                    (number-to-string (plist-get p :high))
                                    (number-to-string (- (plist-get p :high) (plist-get p :low)))
                                    (or (plist-get p :error) "")))))
                  (plist-get d :partitions)))))

(defun kafka-topics-describe ()
  "Describe the topic at point."
  (interactive)
  (kafka-topic kafka-connection (kafka-topics--at-point)))

(defun kafka-topic (name topic)
  "Describe TOPIC on connection NAME."
  (interactive (list (kafka-read-connection) (read-string "Topic: ")))
  (kafka-ensure-connected name)
  (with-current-buffer (get-buffer-create (format "*Kafka Topic: %s/%s*" name topic))
    (kafka-topic-mode)
    (setq kafka-connection name kafka-topic--name topic)
    (revert-buffer)
    (pop-to-buffer (current-buffer))))

(defun kafka-topic-records ()
  "Browse all partitions of this topic."
  (interactive)
  (kafka-records kafka-connection kafka-topic--name))

(defun kafka-topic-records-partition ()
  "Browse the partition at point."
  (interactive)
  (kafka-records kafka-connection kafka-topic--name (list (tabulated-list-get-id))))

(defun kafka-topic-produce ()
  "Produce to this topic."
  (interactive)
  (kafka-produce kafka-connection kafka-topic--name (tabulated-list-get-id)))

(defun kafka-topic-configs ()
  "Show this topic's configuration."
  (interactive)
  (kafka-config-show kafka-connection 'topic kafka-topic--name))

(defun kafka-topic-add-partitions (total)
  "Increase this topic's partition count to TOTAL."
  (interactive
   (list (read-number (format "New total partition count (currently %d): "
                              (length (plist-get kafka-topic--detail :partitions))))))
  (kafka-confirm-write kafka-connection
                       (format "Increase %s to %d partitions (cannot be undone; changes key->partition mapping)"
                               kafka-topic--name total))
  (kafka-rpc-request :kafka/topic/addPartitions
                     (list :connectionId kafka-connection :topic kafka-topic--name :totalCount total))
  (revert-buffer))

;;;; Config view

(defvar-local kafka-config--kind nil)
(defvar-local kafka-config--target nil)
(defvar-local kafka-config--show-defaults t)

(defvar-keymap kafka-config-mode-map
  :parent tabulated-list-mode-map
  "e" #'kafka-config-edit
  "RET" #'kafka-config-edit
  "d" #'kafka-config-toggle-defaults
  "g" #'revert-buffer)

(define-derived-mode kafka-config-mode tabulated-list-mode "Kafka Config"
  "Major mode showing topic or broker configuration."
  (setq tabulated-list-format [("Name" 45 t) ("Value" 40 nil) ("Source" 22 t) ("" 0 nil)])
  (setq tabulated-list-padding 1)
  (add-hook 'tabulated-list-revert-hook #'kafka-config--refresh nil t)
  (tabulated-list-init-header))

(defun kafka-config--refresh ()
  (let ((entries (if (eq kafka-config--kind 'topic)
                     (kafka-rpc-request :kafka/topic/configs
                                        (list :connectionId kafka-connection :topic kafka-config--target))
                   (kafka-rpc-request :kafka/broker/configs
                                      (list :connectionId kafka-connection :brokerId kafka-config--target)))))
    (setq header-line-format
          (kafka-header-line kafka-connection (format "%s %s" kafka-config--kind kafka-config--target)
                             (if kafka-config--show-defaults "" "(overrides only)")))
    (setq tabulated-list-entries
          (delq nil
                (mapcar (lambda (e)
                          (let ((default (kafka-json-bool (plist-get e :default))))
                            (when (or kafka-config--show-defaults (not default))
                              (list (plist-get e :name)
                                    (vector (propertize (plist-get e :name) 'face (if default 'shadow 'bold))
                                            (cond ((kafka-json-bool (plist-get e :sensitive))
                                                   (propertize "<sensitive>" 'face 'warning))
                                                  (t (or (plist-get e :value) "")))
                                            (plist-get e :source)
                                            (if (kafka-json-bool (plist-get e :readOnly)) "ro" ""))))))
                        entries)))))

(defun kafka-config-show (name kind target)
  "Show configuration of KIND (`topic' or `broker') TARGET on NAME."
  (kafka-ensure-connected name)
  (with-current-buffer (get-buffer-create (format "*Kafka Config: %s %s %s*" name kind target))
    (kafka-config-mode)
    (setq kafka-connection name kafka-config--kind kind kafka-config--target target)
    (revert-buffer)
    (pop-to-buffer (current-buffer))))

(defun kafka-config-toggle-defaults ()
  "Toggle display of default-valued entries."
  (interactive)
  (setq kafka-config--show-defaults (not kafka-config--show-defaults))
  (revert-buffer))

(defun kafka-config-edit ()
  "Change the config entry at point (topics only).
Shows current and proposed values and validates on the broker before
applying."
  (interactive)
  (unless (eq kafka-config--kind 'topic)
    (user-error "Only topic configs can be edited (broker config changes are not yet supported)"))
  (let* ((key (or (tabulated-list-get-id) (user-error "No entry here")))
         (cur (aref (tabulated-list-get-entry) 1))
         (new (read-string (format "%s (current: %s): " key cur) cur)))
    (when (equal new cur) (user-error "Unchanged"))
    (kafka-confirm-write kafka-connection (format "Set %s.%s: %s -> %s" kafka-config--target key cur new))
    (let ((params (list :connectionId kafka-connection :topic kafka-config--target
                        :configs (list (intern (concat ":" key)) new))))
      (kafka-rpc-request :kafka/topic/alterConfig (append params '(:validateOnly t)))
      (kafka-rpc-request :kafka/topic/alterConfig params))
    (message "Updated %s" key)
    (run-at-time 0.5 nil (let ((b (current-buffer)))
                           (lambda () (when (buffer-live-p b)
                                        (with-current-buffer b (revert-buffer))))))))

(provide 'kafka-topics)
;;; kafka-topics.el ends here
