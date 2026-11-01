;;; kafka-workflows.el --- Bookmarks, queries, search and diff -*- lexical-binding: t; -*-

;; Copyright (C) 2026 kafka.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:
;; Small workflows composed from the ordinary record browser and RPC reader.

;;; Code:
(require 'bookmark)
(require 'cl-lib)
(require 'ediff)
(require 'kafka-records)
(require 'kafka-topics)
(require 'kafka-groups)

(defvar kafka-connection)
(declare-function kafka-ensure-connected "kafka")
(declare-function kafka-read-connection "kafka")
(declare-function kafka-bytes-string "kafka")

(defcustom kafka-saved-queries nil
  "Saved queries as (NAME CONNECTION TOPIC POSITION FILTER).
POSITION is a start-position plist.  FILTER is a key/value regexp or nil."
  :type '(repeat (list string string string plist (choice (const nil) regexp)))
  :group 'kafka)

(defun kafka-workflows-bookmark-make-record ()
  "Capture the current Kafka buffer without any connection secrets."
  `(,(buffer-name)
    (handler . kafka-workflows-bookmark-handler)
    (kafka-mode . ,major-mode)
    (kafka-connection . ,kafka-connection)
    (kafka-topic . ,(if (eq major-mode 'kafka-records-mode)
                        kafka-records--topic kafka-topic--name))
    (kafka-group . ,kafka-group--id)
    (kafka-partitions . ,kafka-records--partitions)
    (kafka-position . ,kafka-records--position)
    (kafka-filter . ,(if (eq major-mode 'kafka-topics-mode)
                         kafka-topics--filter kafka-records--filter))))

;;;###autoload
(defun kafka-workflows-bookmark-handler (bookmark)
  "Restore a Kafka BOOKMARK through its named connection profile."
  (let ((name (bookmark-prop-get bookmark 'kafka-connection))
        (topic (bookmark-prop-get bookmark 'kafka-topic))
        (filter (bookmark-prop-get bookmark 'kafka-filter)))
    (pcase (bookmark-prop-get bookmark 'kafka-mode)
      ('kafka-records-mode
       (set-buffer (kafka-records name topic
                                  (bookmark-prop-get bookmark 'kafka-partitions)
                                  (bookmark-prop-get bookmark 'kafka-position)))
       (setq kafka-records--filter filter)
       (kafka-records--render))
      ('kafka-topic-mode (set-buffer (kafka-topic name topic)))
      ('kafka-topics-mode
       (set-buffer (kafka-topics name))
       (setq kafka-topics--filter filter)
       (revert-buffer))
      ('kafka-group-mode (set-buffer (kafka-group name (bookmark-prop-get bookmark 'kafka-group))))
      ('kafka-groups-mode (set-buffer (kafka-groups name)))
      (_ (user-error "Not a supported Kafka bookmark")))))

(defun kafka-workflows--bookmark-enable ()
  (setq-local bookmark-make-record-function #'kafka-workflows-bookmark-make-record))

(dolist (hook '(kafka-records-mode-hook kafka-topic-mode-hook kafka-topics-mode-hook
               kafka-group-mode-hook kafka-groups-mode-hook))
  (add-hook hook #'kafka-workflows--bookmark-enable))

;;;###autoload
(defun kafka-run-saved-query (name)
  "Open saved query NAME in a records buffer."
  (interactive (list (completing-read "Kafka query: " kafka-saved-queries nil t)))
  (pcase (assoc name kafka-saved-queries)
    (`(,_ ,connection ,topic ,position ,filter)
     (with-current-buffer (kafka-records connection topic nil position)
       (setq kafka-records--filter filter)
       (kafka-records--render)
       (current-buffer)))
    (_ (user-error "Unknown saved query: %s" name))))

(defvar-local kafka-workflows--marked nil
  "Records selected for diff in this buffer.")

(defun kafka-records-mark ()
  "Toggle the record at point for comparison with `kafka-records-diff'."
  (interactive)
  (let ((record (kafka-records--at-point)))
    (if (member record kafka-workflows--marked)
        (progn (setq kafka-workflows--marked (delete record kafka-workflows--marked))
               (tabulated-list-put-tag " " t))
      (when (= (length kafka-workflows--marked) 2)
        (user-error "Two records already marked, unmark one first"))
      (push record kafka-workflows--marked)
      (tabulated-list-put-tag "*" t))))

(defun kafka-records-diff ()
  "Ediff pretty-printed values of the two records marked with m."
  (interactive)
  (unless (= (length kafka-workflows--marked) 2)
    (user-error "Mark exactly two records with m first"))
  (let ((buffers
         (mapcar (lambda (record)
                   (with-current-buffer (generate-new-buffer "*Kafka record diff*")
                     (insert (kafka-records--pretty
                              (or (kafka-bytes-string (plist-get record :value)) "null")))
                     (goto-char (point-min))
                     (current-buffer)))
                 (reverse kafka-workflows--marked))))
    (ediff-buffers (car buffers) (cadr buffers))))

(defun kafka-workflows--record-matches (regexp record)
  (seq-some (lambda (text) (and text (string-match-p regexp text)))
            (append (list (kafka-bytes-string (plist-get record :key))
                          (kafka-bytes-string (plist-get record :value)))
                    (seq-mapcat (lambda (header)
                                  (list (plist-get header :key)
                                        (kafka-bytes-string (plist-get header :value))))
                                (plist-get record :headers)))))

;;;###autoload
(defun kafka-records-search (name topic position limit regexp)
  "Fetch up to LIMIT records from POSITION in TOPIC on NAME.
Display records whose key, value, header name or value matches REGEXP.
This is a bounded client-side search, not a broker-side index."
  (interactive (list (kafka-read-connection) (read-string "Topic: " kafka-records--topic)
                     (kafka-records-read-position)
                     (read-number "Maximum records to search: " kafka-records-fetch-limit)
                     (read-regexp "Search key/value/headers: ")))
  (unless (<= 1 limit 10000) (user-error "Search limit must be 1..10000"))
  (string-match-p regexp "")
  (kafka-ensure-connected name)
  (let* ((result (kafka-rpc-request :kafka/records/fetch
                                   (list :connectionId name :topic topic
                                         :startPosition position :limit limit)))
         (records (plist-get result :records))
         (matches (seq-filter (lambda (record) (kafka-workflows--record-matches regexp record)) records)))
    (with-current-buffer (generate-new-buffer (format "*Kafka Search: %s/%s*" name topic))
      (kafka-records-mode)
      (setq-local kafka-connection name)
      (setq kafka-records--topic topic kafka-records--position position
            kafka-records--status (format "%d matches in %d fetched%s%s" (length matches) (length records)
                                         (if (eq (plist-get result :truncated) t) " (limit reached)" "")
                                         (if (eq (plist-get result :timedOut) t) " (timed out)" "")))
      (kafka-records--add matches)
      (kafka-records--render)
      (pop-to-buffer (current-buffer))
      (current-buffer))))

(with-eval-after-load 'kafka-records
  (keymap-set kafka-records-mode-map "m" #'kafka-records-mark)
  (keymap-set kafka-records-mode-map "=" #'kafka-records-diff))

(provide 'kafka-workflows)
;;; kafka-workflows.el ends here
