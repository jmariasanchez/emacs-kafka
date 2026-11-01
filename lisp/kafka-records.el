;;; kafka-records.el --- Record browser and live tail  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 kafka.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; `kafka-records-mode' shows records fetched from a topic, either as a
;; bounded one-shot read or as a live stream.  Reading never commits offsets
;; to any consumer group: the bridge uses a dedicated, manually assigned,
;; non-committing consumer.
;;
;; Live streams use credit-based flow control.  Each notification batch
;; consumes one credit; the buffer returns credit only after rendering, so a
;; fast topic cannot flood Emacs.  The buffer keeps at most
;; `kafka-records-max-rows' rows and says so when older rows are dropped.

;;; Code:

(require 'kafka-rpc)
(require 'tabulated-list)
(require 'json)

(defvar kafka-connection)
(declare-function kafka-ensure-connected "kafka")
(declare-function kafka-read-connection "kafka")
(declare-function kafka-header-line "kafka")
(declare-function kafka-bytes-display "kafka")
(declare-function kafka-bytes-string "kafka")
(declare-function kafka-format-time "kafka")
(declare-function kafka-json-bool "kafka")
(declare-function kafka-produce-from-record "kafka-producer")
(autoload 'kafka-schema-registry-record-detail "kafka-schema-registry")

(defcustom kafka-records-decoders '(kafka-schema-registry-record-detail)
  "Functions called with connection and record to return detail text or nil."
  :type 'hook :group 'kafka)

(defcustom kafka-records-fetch-limit 200
  "Default number of records for a one-shot fetch."
  :type 'integer :group 'kafka)

(defcustom kafka-records-max-rows 5000
  "Maximum rows kept in a records buffer.  Oldest rows are dropped."
  :type 'integer :group 'kafka)

(defcustom kafka-records-batch-size 200
  "Records per live-stream batch."
  :type 'integer :group 'kafka)

(defcustom kafka-records-default-position '(:type "tail" :count 50)
  "Default start position for record browsing."
  :type 'plist :group 'kafka)

(defvar-local kafka-records--topic nil)
(defvar-local kafka-records--partitions nil)
(defvar-local kafka-records--position nil)
(defvar-local kafka-records--rows nil "List of record plists, newest first.")
(defvar-local kafka-records--count 0)
(defvar-local kafka-records--dropped 0)
(defvar-local kafka-records--sub nil "Live subscription id or nil.")
(defvar-local kafka-records--state nil)
(defvar-local kafka-records--status nil)
(defvar-local kafka-records--filter nil)

(defvar kafka-records--subs (make-hash-table :test #'equal)
  "Map subscription id -> records buffer.")

(defvar kafka-records--next-sub 0)

(defvar-keymap kafka-records-mode-map
  :parent tabulated-list-mode-map
  "RET" #'kafka-records-show
  "f" #'kafka-records-fetch
  "s" #'kafka-records-seek
  "l" #'kafka-records-live
  "k" #'kafka-records-stop
  "/" #'kafka-records-filter
  "c" #'kafka-records-clear
  "w" #'kafka-records-copy-value
  "P" #'kafka-records-reproduce
  "g" #'kafka-records-fetch)

(define-derived-mode kafka-records-mode tabulated-list-mode "Kafka Records"
  "Major mode for browsing Kafka records.
\\{kafka-records-mode-map}"
  (setq tabulated-list-format [("P" 4 kafka-records--sort-p :right-align t)
                               ("Offset" 10 kafka-records--sort-offset :right-align t)
                               ("Timestamp" 20 t)
                               ("Key" 24 t)
                               ("Value" 0 nil)])
  (setq tabulated-list-padding 1)
  (add-hook 'kill-buffer-hook #'kafka-records--stop-quietly nil t)
  (tabulated-list-init-header))

(defun kafka-records--sort-p (a b)
  (< (plist-get (car a) :partition) (plist-get (car b) :partition)))
(defun kafka-records--sort-offset (a b)
  (< (plist-get (car a) :offset) (plist-get (car b) :offset)))

(defun kafka-records--matches (r)
  (or (null kafka-records--filter)
      (let ((case-fold-search t))
        (or (string-match-p kafka-records--filter (or (kafka-bytes-string (plist-get r :value)) ""))
            (string-match-p kafka-records--filter (or (kafka-bytes-string (plist-get r :key)) ""))))))

(defun kafka-records--entry (r)
  (list r (vector (number-to-string (plist-get r :partition))
                  (number-to-string (plist-get r :offset))
                  (kafka-format-time (plist-get r :timestamp))
                  (kafka-bytes-display (plist-get r :key) 24)
                  (kafka-bytes-display (plist-get r :value) 200))))

(defun kafka-records--update-header ()
  (setq header-line-format
        (kafka-header-line
         kafka-connection
         (propertize kafka-records--topic 'face 'font-lock-constant-face)
         (if kafka-records--partitions
             (format "partitions %s" (mapconcat #'number-to-string kafka-records--partitions ","))
           "all partitions")
         (format "from %s" (kafka-records--position-string kafka-records--position))
         (format "%d rows" (length kafka-records--rows))
         (if (> kafka-records--dropped 0)
             (propertize (format "(%d older rows dropped)" kafka-records--dropped) 'face 'warning)
           "")
         (if kafka-records--filter (format "filter: %s" kafka-records--filter) "")
         (pcase kafka-records--state
           ("active" (propertize "● live" 'face 'success))
           ("paused" (propertize "● live (catching up)" 'face 'warning))
           ("error" (propertize (format "● error: %s" kafka-records--status) 'face 'error))
           (_ (or kafka-records--status ""))))))

(defun kafka-records--render ()
  (let ((rows (seq-filter #'kafka-records--matches kafka-records--rows)))
    (setq tabulated-list-entries (mapcar #'kafka-records--entry rows)))
  (kafka-records--update-header)
  (tabulated-list-print t))

(defun kafka-records--add (records &optional newest-first)
  "Add RECORDS (a vector) to the buffer, enforcing the row cap.
Fetched batches arrive oldest-first; NEWEST-FIRST is unused for now."
  (ignore newest-first)
  (seq-doseq (r records) (push r kafka-records--rows))
  (let ((n (length kafka-records--rows)))
    (when (> n kafka-records-max-rows)
      (setq kafka-records--dropped (+ kafka-records--dropped (- n kafka-records-max-rows)))
      (setq kafka-records--rows (seq-take kafka-records--rows kafka-records-max-rows)))))

(defun kafka-records--position-string (pos)
  (pcase (plist-get pos :type)
    ("tail" (format "last %d/partition" (plist-get pos :count)))
    ("offset" (format "offset %d" (plist-get pos :offset)))
    ("timestamp" (kafka-format-time (plist-get pos :timestamp)))
    (other other)))

;;;###autoload
(defun kafka-records (name topic &optional partitions position)
  "Browse records of TOPIC on NAME, optionally restricted to PARTITIONS.
POSITION is a start-position plist; defaults to
`kafka-records-default-position'."
  (interactive (list (kafka-read-connection) (read-string "Topic: ")))
  (kafka-ensure-connected name)
  (with-current-buffer (get-buffer-create
                        (format "*Kafka Records: %s/%s%s*" name topic
                                (if partitions (format "[%s]" (mapconcat #'number-to-string partitions ",")) "")))
    (unless (derived-mode-p 'kafka-records-mode) (kafka-records-mode))
    (setq kafka-connection name
          kafka-records--topic topic
          kafka-records--partitions partitions
          kafka-records--position (or position kafka-records-default-position))
    (pop-to-buffer (current-buffer))
    (kafka-records-fetch)
    (current-buffer)))

(defun kafka-records-fetch ()
  "Fetch a bounded batch of records from the current position (async)."
  (interactive)
  (kafka-records--stop-quietly)
  (let ((buf (current-buffer)))
    (setq kafka-records--status "fetching…"
          kafka-records--rows nil
          kafka-records--dropped 0)
    (kafka-records--render)
    (kafka-rpc-async
     :kafka/records/fetch
     (list :connectionId kafka-connection :topic kafka-records--topic
           :partitions (and kafka-records--partitions (vconcat kafka-records--partitions))
           :startPosition kafka-records--position
           :limit kafka-records-fetch-limit)
     (lambda (res)
       (when (buffer-live-p buf)
         (with-current-buffer buf
           (kafka-records--add (plist-get res :records))
           (setq kafka-records--status
                 (concat (format "fetched %d" (length (plist-get res :records)))
                         (when (kafka-json-bool (plist-get res :truncated))
                           (propertize " (limit reached; more available)" 'face 'warning))
                         (when (kafka-json-bool (plist-get res :timedOut))
                           (propertize " (timed out)" 'face 'warning))))
           (kafka-records--render))))
     (lambda (msg _code _data)
       (when (buffer-live-p buf)
         (with-current-buffer buf
           (setq kafka-records--status (propertize msg 'face 'error))
           (kafka-records--render)))))))

(defun kafka-records-read-position ()
  "Read a start position from the user."
  (pcase (completing-read "Start from: " '("tail" "earliest" "latest" "offset" "timestamp") nil t)
    ("tail" (list :type "tail" :count (read-number "Records per partition: " 50)))
    ("earliest" '(:type "earliest"))
    ("latest" '(:type "latest"))
    ("offset" (list :type "offset" :offset (read-number "Offset: " 0)))
    ("timestamp"
     (let ((s (read-string "Timestamp (e.g. 2026-10-31 12:00, or -15m / -2h / -1d): ")))
       (list :type "timestamp" :timestamp (kafka-records--parse-time s))))))

(defun kafka-records--parse-time (s)
  "Parse S as absolute time or relative offset like -15m.  Return epoch ms."
  (if (string-match "\\`-\\([0-9]+\\)\\([smhd]\\)\\'" s)
      (let ((n (string-to-number (match-string 1 s)))
            (unit (pcase (match-string 2 s) ("s" 1) ("m" 60) ("h" 3600) ("d" 86400))))
        (truncate (* 1000 (- (float-time) (* n unit)))))
    (truncate (* 1000 (float-time (date-to-time s))))))

(defun kafka-records-seek (pos)
  "Set start position POS and refetch."
  (interactive (list (kafka-records-read-position)))
  (setq kafka-records--position pos)
  (if kafka-records--sub (kafka-records-live) (kafka-records-fetch)))

(defun kafka-records-filter (re)
  "Show only records whose key or value matches RE (empty clears).
This is a display filter over already-received rows."
  (interactive (list (read-regexp "Filter (regexp): " kafka-records--filter)))
  (setq kafka-records--filter (if (string-empty-p re) nil re))
  (kafka-records--render))

(defun kafka-records-clear ()
  "Remove all rows from the buffer."
  (interactive)
  (setq kafka-records--rows nil kafka-records--dropped 0)
  (kafka-records--render))

;;;; Live streaming

(defun kafka-records-live ()
  "Start a live stream from the current position."
  (interactive)
  (kafka-records--stop-quietly)
  (setq kafka-records--rows nil kafka-records--dropped 0
        kafka-records--state "starting" kafka-records--status nil)
  ;; Register before the request: notifications may precede the response.
  (let ((sid (format "el-%d-%d" (emacs-pid) (cl-incf kafka-records--next-sub))))
    (setq kafka-records--sub sid)
    (puthash sid (current-buffer) kafka-records--subs)
    (condition-case err
        (kafka-rpc-request
         :kafka/consume/start
         (list :connectionId kafka-connection :topic kafka-records--topic
               :subscriptionId sid
               :partitions (and kafka-records--partitions (vconcat kafka-records--partitions))
               :startPosition kafka-records--position
               :batchSize kafka-records-batch-size
               :initialCredit 4))
      (error (remhash sid kafka-records--subs)
             (setq kafka-records--sub nil kafka-records--state "error"
                   kafka-records--status (error-message-string err))
             (kafka-records--render)
             (signal (car err) (cdr err))))
    (unless (member kafka-records--state '("active" "paused"))
      (setq kafka-records--state "active"))
    (kafka-records--render)
    (message "Streaming %s (k to stop)" kafka-records--topic)))

(defun kafka-records--stop-quietly ()
  (when kafka-records--sub
    (let ((sid kafka-records--sub))
      (setq kafka-records--sub nil kafka-records--state nil)
      (remhash sid kafka-records--subs)
      (when (kafka-rpc-live-p)
        (ignore-errors
          (kafka-rpc-async :kafka/consume/stop (list :subscriptionId sid) #'ignore #'ignore))))))

(defun kafka-records-stop ()
  "Stop the live stream."
  (interactive)
  (unless kafka-records--sub (user-error "Not streaming"))
  (kafka-records--stop-quietly)
  (setq kafka-records--status "stopped")
  (kafka-records--render))

(defun kafka-records--on-batch (params)
  (let* ((sid (plist-get params :subscriptionId))
         (buf (gethash sid kafka-records--subs)))
    (if (not (buffer-live-p buf))
        (progn (remhash sid kafka-records--subs)
               (kafka-rpc-async :kafka/consume/stop (list :subscriptionId sid) #'ignore #'ignore))
      (with-current-buffer buf
        (kafka-records--add (plist-get params :records))
        (kafka-records--render))
      ;; Return credit only after rendering: this is the back-pressure.
      (kafka-rpc-async :kafka/consume/credit (list :subscriptionId sid :batches 1) #'ignore #'ignore))))

(defun kafka-records--on-state (params)
  (let* ((sid (plist-get params :subscriptionId))
         (buf (gethash sid kafka-records--subs))
         (state (plist-get params :state)))
    (when (buffer-live-p buf)
      (with-current-buffer buf
        (setq kafka-records--state state
              kafka-records--status (plist-get params :message))
        (when (equal state "cancelled")
          (remhash sid kafka-records--subs)
          (setq kafka-records--sub nil kafka-records--status "stopped"))
        (kafka-records--update-header)
        (force-mode-line-update)))))

(kafka-rpc-on-notification 'kafka/consume/records #'kafka-records--on-batch)
(kafka-rpc-on-notification 'kafka/consume/state #'kafka-records--on-state)

(add-hook 'kafka-rpc-shutdown-hook
          (lambda ()
            (maphash (lambda (_ buf)
                       (when (buffer-live-p buf)
                         (with-current-buffer buf
                           (setq kafka-records--sub nil kafka-records--state "error"
                                 kafka-records--status "bridge exited")
                           (kafka-records--update-header))))
                     kafka-records--subs)
            (clrhash kafka-records--subs)))

;;;; Record detail

(defun kafka-records--at-point ()
  (or (tabulated-list-get-id) (user-error "No record here")))

(defun kafka-record-insert (r)
  "Insert a full description of record R at point."
  (insert (propertize (format "%s / partition %d / offset %d\n"
                              (plist-get r :topic) (plist-get r :partition) (plist-get r :offset))
                      'face 'bold))
  (insert (format "Timestamp: %s (%s, %s)\n" (kafka-format-time (plist-get r :timestamp))
                  (plist-get r :timestamp) (plist-get r :timestampType)))
  (let ((h (plist-get r :headers)))
    (when (> (length h) 0)
      (insert "Headers:\n")
      (seq-doseq (x h)
        (insert (format "  %s: %s\n" (plist-get x :key) (kafka-bytes-display (plist-get x :value)))))))
  (dolist (part '(:key :value))
    (let ((b (plist-get r part)))
      (insert "\n" (propertize (format "%s%s:\n" (capitalize (substring (symbol-name part) 1))
                                       (if b (format " (%s, %d bytes)" (plist-get b :encoding) (plist-get b :size)) ""))
                               'face 'font-lock-keyword-face))
      (cond ((null b) (insert (propertize "null\n" 'face 'shadow)))
            ((equal (plist-get b :encoding) "base64")
             (insert (kafka-records--hexdump (base64-decode-string (plist-get b :data)))))
            (t (insert (kafka-records--pretty (plist-get b :data)) "\n"))))))

(defun kafka-records--pretty (s)
  "Pretty-print S if it is JSON, otherwise return it unchanged."
  (condition-case nil
      (if (string-match-p "\\`[ \t\n]*[[{]" s)
          (with-temp-buffer
            (insert s)
            (json-pretty-print-buffer)
            (buffer-string))
        s)
    (error s)))

(defun kafka-records--hexdump (bytes)
  "Return a hex dump of unibyte string BYTES."
  (with-temp-buffer
    (let ((i 0) (n (length bytes)))
      (while (< i n)
        (let ((chunk (substring bytes i (min n (+ i 16)))))
          (insert (format "%08x  %-48s %s\n" i
                          (mapconcat (lambda (c) (format "%02x" c)) chunk " ")
                          (mapconcat (lambda (c) (if (and (>= c 32) (< c 127)) (string c) ".")) chunk ""))))
        (setq i (+ i 16))))
    (buffer-string)))

(defun kafka-records-show ()
  "Show the record at point in full."
  (interactive)
  (let ((r (kafka-records--at-point))
        (conn kafka-connection))
    (with-current-buffer (get-buffer-create "*Kafka Record*")
      (let ((inhibit-read-only t))
        (erase-buffer)
        (kafka-record-insert r)
        (dolist (decoder kafka-records-decoders)
          (when-let* ((text (funcall decoder conn r))) (insert text)))
        (special-mode)
        (setq-local kafka-connection conn)
        (goto-char (point-min)))
      (display-buffer (current-buffer)))))

(defun kafka-records-copy-value ()
  "Copy the value of the record at point."
  (interactive)
  (let ((v (kafka-bytes-string (plist-get (kafka-records--at-point) :value))))
    (kill-new (or v ""))
    (message "Copied %d bytes" (length v))))

(defun kafka-records-reproduce ()
  "Open a producer buffer prefilled from the record at point."
  (interactive)
  (kafka-produce-from-record kafka-connection (kafka-records--at-point)))

(provide 'kafka-records)
;;; kafka-records.el ends here
