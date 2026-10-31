;;; kafka-producer.el --- Compose and publish records  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 kafka.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; A producer buffer is a normal editing buffer with a small header:
;;
;;   Topic: orders
;;   Partition: auto
;;   Key: order-42
;;   Header: source=emacs
;;   --text follows this line--
;;   {"id": 42}
;;
;; `C-c C-c' sends, `C-c C-k' discards.  The body is sent verbatim as UTF-8.
;; A `Key:' line that is absent means a null key; an empty body with
;; `Tombstone: yes' sends a null value.

;;; Code:

(require 'kafka-rpc)

(defvar kafka-connection)
(declare-function kafka-ensure-connected "kafka")
(declare-function kafka-read-connection "kafka")
(declare-function kafka-confirm-write "kafka")
(declare-function kafka-header-line "kafka")
(declare-function kafka-bytes-string "kafka")

(defconst kafka-producer-separator "--text follows this line--")

(defvar-keymap kafka-producer-mode-map
  "C-c C-c" #'kafka-producer-send
  "C-c C-s" #'kafka-producer-send-and-keep
  "C-c C-k" #'kafka-producer-discard)

(defvar kafka-producer-font-lock
  `((,(concat "^" (regexp-quote kafka-producer-separator) "$") . 'shadow)
    ("^\\(Topic\\|Partition\\|Key\\|Header\\|Tombstone\\|Timestamp\\):" . 'font-lock-keyword-face)))

(define-derived-mode kafka-producer-mode text-mode "Kafka Produce"
  "Major mode for composing a Kafka record.
\\{kafka-producer-mode-map}"
  (setq-local font-lock-defaults '(kafka-producer-font-lock)))

;;;###autoload
(defun kafka-produce (name topic &optional partition key value headers)
  "Open a buffer to compose a record for TOPIC on NAME."
  (interactive (list (kafka-read-connection) (read-string "Topic: ")))
  (when (plist-get (cdr (assoc name (bound-and-true-p kafka-clusters))) :read-only)
    (user-error "Connection %s is read-only" name))
  (with-current-buffer (generate-new-buffer (format "*Kafka Produce: %s/%s*" name topic))
    (kafka-producer-mode)
    (setq-local kafka-connection name)
    (insert (format "Topic: %s\n" topic)
            (format "Partition: %s\n" (if partition partition "auto"))
            (if key (format "Key: %s\n" key) "Key: \n"))
    (dolist (h headers) (insert (format "Header: %s=%s\n" (car h) (or (cdr h) ""))))
    (insert kafka-producer-separator "\n")
    (let ((body (point)))
      (when value (insert value))
      (setq header-line-format
            (kafka-header-line name "C-c C-c send" "C-c C-s send & keep" "C-c C-k discard"))
      (goto-char (if value (point-min) body))
      (unless value (goto-char body)))
    (pop-to-buffer (current-buffer))))

(defun kafka-produce-from-record (name r)
  "Open a producer prefilled from record R (plist) on NAME."
  (kafka-produce name (plist-get r :topic) nil
                 (kafka-bytes-string (plist-get r :key))
                 (kafka-bytes-string (plist-get r :value))
                 (mapcar (lambda (h) (cons (plist-get h :key) (kafka-bytes-string (plist-get h :value))))
                         (plist-get r :headers))))

(defun kafka-producer-parse ()
  "Parse the current producer buffer into produce params (minus connection)."
  (save-excursion
    (goto-char (point-min))
    (let ((sep (save-excursion
                 (re-search-forward (concat "^" (regexp-quote kafka-producer-separator) "$") nil t)))
          topic partition key headers tombstone timestamp)
      (unless sep (user-error "Missing %S line" kafka-producer-separator))
      (setq sep (save-excursion (goto-char sep) (line-beginning-position)))
      (while (< (point) sep)
        (if (not (looking-at "^\\([A-Za-z]+\\): ?\\(.*\\)$"))
            (unless (looking-at "^[ \t]*$")
              (user-error "Bad header line: %S"
                          (buffer-substring (line-beginning-position) (line-end-position))))
          (let ((k (match-string-no-properties 1)) (v (match-string-no-properties 2)))
            (pcase k
              ("Topic" (setq topic (string-trim v)))
              ("Partition" (setq partition (unless (member (string-trim v) '("" "auto"))
                                             (string-to-number v))))
              ("Key" (setq key (unless (string-empty-p v) v)))
              ("Tombstone" (setq tombstone (member (downcase (string-trim v)) '("yes" "y" "t" "true"))))
              ("Timestamp" (setq timestamp (unless (string-empty-p v) (string-to-number v))))
              ("Header" (if (string-match "\\`\\([^=]+\\)=\\(.*\\)\\'" v)
                            (push (list :key (match-string 1 v) :value (match-string 2 v)) headers)
                          (user-error "Header must be key=value: %S" v)))
              (_ (user-error "Unknown field %S" k)))))
        (forward-line 1))
      (when (or (null topic) (string-empty-p topic)) (user-error "Topic: is required"))
      (goto-char sep)
      (forward-line 1)
      (let ((body (buffer-substring-no-properties (point) (point-max))))
        (when (and tombstone (not (string-empty-p (string-trim body))))
          (user-error "Tombstone records must have an empty body"))
        (list :topic topic
              :partition partition
              :key key
              :value (unless tombstone body)
              :headers (vconcat (nreverse headers))
              :timestamp timestamp)))))

(defun kafka-producer--send (keep)
  (let* ((p (kafka-producer-parse))
         (name kafka-connection))
    (kafka-confirm-write name (format "Publish to %s" (plist-get p :topic)))
    (let ((d (kafka-rpc-request :kafka/produce (append (list :connectionId name) p))))
      (message "Delivered to %s [%d] @ offset %d"
               (plist-get d :topic) (plist-get d :partition) (plist-get d :offset))
      (unless keep (kill-buffer (current-buffer)))
      d)))

(defun kafka-producer-send ()
  "Send the record and close the buffer."
  (interactive)
  (kafka-producer--send nil))

(defun kafka-producer-send-and-keep ()
  "Send the record and keep the buffer for another send."
  (interactive)
  (kafka-producer--send t))

(defun kafka-producer-discard ()
  "Discard this producer buffer."
  (interactive)
  (kill-buffer (current-buffer)))

(provide 'kafka-producer)
;;; kafka-producer.el ends here
