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
(require 'json)
(require 'seq)

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

(defcustom kafka-producer-key-separator "\t"
  "Literal separator between a key and value in region records.
Only the first occurrence splits the line.  Nil disables key splitting."
  :type '(choice (const nil) string) :group 'kafka)

(defun kafka-producer--lines (text)
  "Split TEXT into records, preserving empty lines except the final newline."
  (let ((lines (split-string text "\n" nil)))
    (if (string-suffix-p "\n" text) (butlast lines) lines)))

(defun kafka-producer--line-record (line)
  (let ((separator kafka-producer-key-separator))
    (if (and separator (not (string-empty-p separator))
             (string-match (regexp-quote separator) line))
        (list :key (substring line 0 (match-beginning 0))
              :value (substring line (match-end 0)))
      (list :value line))))

(defun kafka-producer--summary (result)
  (let* ((results (plist-get result :results))
         (success (seq-count (lambda (r) (eq (plist-get r :ok) t)) results)))
    (message "Kafka batch: %d delivered, %d failed%s"
             success (- (length results) success)
             (if (eq (plist-get result :transactional) t)
                 (if (eq (plist-get result :committed) t) " (committed)" " (aborted, none visible to committed readers)")
               ""))))

(defun kafka-producer--batch (name topic records transactional)
  "Publish RECORDS to TOPIC on NAME, optionally TRANSACTIONAL."
  (unless (<= 1 (length records) 10000)
    (user-error "Batch must contain 1..10000 records"))
  (kafka-confirm-write name (format "Publish %d records to %s%s" (length records) topic
                                   (if transactional " transactionally" "")))
  (kafka-ensure-connected name)
  (condition-case err
      (let ((result (kafka-rpc-request
                     :kafka/produce/batch
                     (list :connectionId name :topic topic :records (vconcat records)
                           :transactional (if transactional t :json-false))
                     (max kafka-rpc-timeout 120))))
        (kafka-producer--summary result)
        result)
    (kafka-error
     (when (= (or (nth 2 err) 0) 1005)
       (kafka-producer--summary (nth 3 err)))
     (signal (car err) (cdr err)))))

;;;###autoload
(defun kafka-produce-region (name topic start end &optional transactional)
  "Publish each line between START and END to TOPIC on NAME.
With prefix argument TRANSACTIONAL, commit the entire batch atomically."
  (interactive (list (kafka-read-connection) (read-string "Topic: ")
                     (region-beginning) (region-end) current-prefix-arg))
  (when (= start end) (user-error "Region is empty"))
  (kafka-producer--batch
   name topic (mapcar #'kafka-producer--line-record
                      (kafka-producer--lines (buffer-substring-no-properties start end)))
   transactional))

(defun kafka-producer--file-record (line)
  "Parse one JSON record object or a raw LINE."
  (if (not (string-match-p "\\`[ \t]*{" line))
      (list :value line)
    (let* ((object (json-parse-string line :object-type 'plist :array-type 'list
                                     :null-object nil :false-object :json-false))
           (headers (plist-get object :headers)))
      (when headers
        (setq object
              (plist-put object :headers
                         (vconcat
                          (cl-loop for (key value) on headers by #'cddr
                                   collect (kafka-rpc-clean-params
                                            (list :key (substring (symbol-name key) 1) :value value)))))))
      (if (seq-some (lambda (field) (plist-member object field))
                    '(:key :value :partition :headers :encoding :timestamp))
          (or (kafka-rpc-clean-params object) (make-hash-table :test #'equal))
        (list :value line)))))

;;;###autoload
(defun kafka-produce-file (name topic file &optional transactional)
  "Publish JSON Lines or raw lines from FILE to TOPIC on NAME.
A JSON object with record fields describes key, value, headers, partition,
encoding and timestamp.  Other JSON objects are raw payload lines.
With prefix argument TRANSACTIONAL, commit the entire batch atomically."
  (interactive (list (kafka-read-connection) (read-string "Topic: ")
                     (read-file-name "Records file: " nil nil t) current-prefix-arg))
  (let ((records (with-temp-buffer
                   (insert-file-contents file)
                   (when (= (point-min) (point-max)) (user-error "File is empty"))
                   (mapcar #'kafka-producer--file-record
                           (kafka-producer--lines (buffer-string))))))
    (kafka-producer--batch name topic records transactional)))

(defun kafka-produce-transactional (name topic start end)
  "Publish the region START to END atomically to TOPIC on NAME."
  (interactive (list (kafka-read-connection) (read-string "Topic: ")
                     (region-beginning) (region-end)))
  (kafka-produce-region name topic start end t))

(provide 'kafka-producer)
;;; kafka-producer.el ends here
