;;; kafka.el --- Apache Kafka developer environment  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 kafka.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later
;; Version: 0.1.0
;; Package-Requires: ((emacs "29.1") (jsonrpc "1.0.24"))
;; Keywords: tools, data
;; URL: https://github.com/kafka-el/kafka.el

;;; Commentary:

;; Native Emacs interface to Apache Kafka, backed by the `kafka-bridge'
;; Rust executable.
;;
;; Quick start:
;;
;;   (setq kafka-clusters
;;         '(("local" :bootstrap "127.0.0.1:9092")
;;           ("prod"  :bootstrap "broker.example:9093" :environment "production"
;;                    :read-only t :security-protocol "SASL_SSL"
;;                    :sasl-mechanism "SCRAM-SHA-512" :sasl-username "me")))
;;   M-x kafka
;;
;; Passwords are looked up with `auth-source' (host = connection name, or
;; first bootstrap host; user = :sasl-username) unless :sasl-password is
;; given explicitly.

;;; Code:

(require 'kafka-rpc)
(require 'auth-source)
(require 'tabulated-list)
(require 'subr-x)

(defcustom kafka-clusters nil
  "Named cluster connection profiles.
Each element is (NAME . PLIST).  Recognized keys:
  :bootstrap          bootstrap servers string (required)
  :environment        free-form label, e.g. \"production\"
  :read-only          if non-nil, the bridge refuses every write
  :security-protocol  PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL
  :sasl-mechanism     PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER, GSSAPI
  :sasl-username      SASL user
  :sasl-password      SASL password; prefer auth-source instead
  :ssl-ca-location, :ssl-certificate-location, :ssl-key-location
  :ssl-key-password   prefer auth-source instead
  :timeout-ms         per-request timeout in milliseconds
  :properties         alist of extra librdkafka (KEY . VALUE) properties"
  :type '(alist :key-type string :value-type plist)
  :group 'kafka)

(defcustom kafka-protected-environments '("production" "prod")
  "Environment labels that require explicit confirmation for writes."
  :type '(repeat string)
  :group 'kafka)

(defvar kafka--connected (make-hash-table :test #'equal)
  "Map of connection name to cluster-info plist for live connections.")

(defvar-local kafka-connection nil
  "Connection name the current buffer operates on.")

(add-hook 'kafka-rpc-shutdown-hook (lambda () (clrhash kafka--connected)))

;;;; Profiles and credentials

(defun kafka--profile (name)
  "Return the plist for profile NAME."
  (or (cdr (assoc name kafka-clusters))
      (user-error "No Kafka profile named %S in `kafka-clusters'" name)))

(defun kafka--secret (name plist key user)
  "Return secret KEY from PLIST, or look it up in auth-source for NAME/USER."
  (or (plist-get plist key)
      (when user
        (let* ((host (car (split-string
                           (car (split-string (plist-get plist :bootstrap) ",")) ":")))
               (found (or (car (auth-source-search :host name :user user :max 1))
                          (car (auth-source-search :host host :user user :max 1))))
               (secret (plist-get found :secret)))
          (if (functionp secret) (funcall secret) secret)))))

(defun kafka--connect-params (name)
  "Build `kafka/connect' params for profile NAME."
  (let* ((p (kafka--profile name))
         (user (plist-get p :sasl-username))
         (params
          (list :connectionId name
                :bootstrapServers (or (plist-get p :bootstrap)
                                      (user-error "Profile %s has no :bootstrap" name))
                :readOnly (if (plist-get p :read-only) t :json-false))))
    (cl-loop for (k . wire) in '((:environment . :environment)
                                 (:security-protocol . :securityProtocol)
                                 (:sasl-mechanism . :saslMechanism)
                                 (:sasl-username . :saslUsername)
                                 (:ssl-ca-location . :sslCaLocation)
                                 (:ssl-certificate-location . :sslCertificateLocation)
                                 (:ssl-key-location . :sslKeyLocation)
                                 (:timeout-ms . :timeoutMs))
             for v = (plist-get p k)
             when v do (setq params (plist-put params wire v)))
    (when-let* ((pw (and (plist-get p :sasl-mechanism)
                         (kafka--secret name p :sasl-password user))))
      (setq params (plist-put params :saslPassword pw)))
    (when-let* ((kp (plist-get p :ssl-key-password)))
      (setq params (plist-put params :sslKeyPassword kp)))
    (when-let* ((props (plist-get p :properties)))
      (setq params (plist-put params :properties
                              (cl-loop for (k . v) in props
                                       append (list (intern (concat ":" k)) v)))))
    params))

(defun kafka-connected-p (name)
  "Return non-nil when NAME is connected."
  (and (kafka-rpc-live-p) (gethash name kafka--connected)))

(defun kafka-ensure-connected (name)
  "Connect to NAME unless already connected.  Return cluster info."
  (or (kafka-connected-p name)
      (let ((info (kafka-rpc-request :kafka/connect (kafka--connect-params name))))
        (puthash name info kafka--connected)
        info)))

(defun kafka-read-connection (&optional prompt)
  "Read a connection name, defaulting to the buffer's connection."
  (or (and (not current-prefix-arg) kafka-connection)
      (let ((names (mapcar #'car kafka-clusters)))
        (unless names (user-error "Configure `kafka-clusters' first"))
        (completing-read (or prompt "Kafka cluster: ") names nil t nil nil
                         (car names)))))

(defun kafka-protected-p (name)
  "Return non-nil when NAME is a protected environment."
  (member (plist-get (kafka--profile name) :environment) kafka-protected-environments))

(defun kafka-read-only-p (name)
  "Return non-nil when NAME is a read-only profile."
  (plist-get (kafka--profile name) :read-only))

(defun kafka-confirm-write (name what)
  "Ask for confirmation to perform WHAT on NAME when it is protected."
  (when (kafka-read-only-p name)
    (user-error "Connection %s is read-only" name))
  (when (kafka-protected-p name)
    (unless (yes-or-no-p (format "[%s: %s] %s? " name
                                 (plist-get (kafka--profile name) :environment) what))
      (user-error "Cancelled"))))

(defun kafka-confirm-exact (name what thing)
  "Require the user to type THING exactly to confirm WHAT on NAME."
  (when (kafka-read-only-p name)
    (user-error "Connection %s is read-only" name))
  (let ((typed (read-string (format "[%s] %s. Type %S to confirm: " name what thing))))
    (unless (string= typed thing)
      (user-error "Confirmation did not match; nothing done"))
    typed))

;;;; Shared rendering helpers

(defun kafka-json-bool (v)
  "Convert JSON boolean V to a Lisp boolean."
  (and v (not (eq v :json-false))))

(defun kafka-format-time (ms)
  "Format epoch milliseconds MS."
  (if (numberp ms)
      (format-time-string "%F %T" (seconds-to-time (/ ms 1000.0)))
    ""))

(defun kafka-bytes-display (b &optional limit)
  "Return a display string for wire-encoded bytes B, truncated to LIMIT."
  (cond
   ((null b) (propertize "null" 'face 'shadow))
   ((equal (plist-get b :encoding) "base64")
    (propertize (format "<binary %d bytes>" (plist-get b :size)) 'face 'warning))
   (t (let ((s (replace-regexp-in-string "[\n\t]" " " (plist-get b :data))))
        (if (and limit (> (length s) limit))
            (concat (substring s 0 limit) "…")
          s)))))

(defun kafka-bytes-string (b)
  "Return the full content of wire bytes B as a (possibly unibyte) string."
  (cond ((null b) nil)
        ((equal (plist-get b :encoding) "base64")
         (base64-decode-string (plist-get b :data)))
        (t (plist-get b :data))))

(defun kafka-header-line (name &rest extra)
  "Header line for connection NAME plus EXTRA strings."
  (let* ((p (ignore-errors (kafka--profile name)))
         (env (plist-get p :environment)))
    (concat " " (propertize name 'face 'bold)
            (when env (concat " " (propertize (format "[%s]" env)
                                              'face (if (member env kafka-protected-environments)
                                                        'error 'success))))
            (when (plist-get p :read-only) (propertize " read-only" 'face 'warning))
            (when extra (concat "  " (string-join extra "  "))))))

;;;; Cluster list

(defvar-keymap kafka-clusters-mode-map
  :parent tabulated-list-mode-map
  "RET" #'kafka-clusters-open
  "t" #'kafka-clusters-open
  "G" #'kafka-clusters-groups
  "b" #'kafka-clusters-brokers
  "c" #'kafka-clusters-connect
  "d" #'kafka-clusters-disconnect
  "g" #'revert-buffer)

(define-derived-mode kafka-clusters-mode tabulated-list-mode "Kafka Clusters"
  "Major mode listing configured Kafka clusters."
  (setq tabulated-list-format [("Name" 18 t) ("Status" 12 t) ("Env" 12 t)
                               ("Mode" 10 t) ("Bootstrap" 0 t)])
  (setq tabulated-list-padding 1)
  (add-hook 'tabulated-list-revert-hook #'kafka-clusters--refresh nil t)
  (tabulated-list-init-header))

(defun kafka-clusters--refresh ()
  (setq tabulated-list-entries
        (mapcar (lambda (c)
                  (let* ((name (car c)) (p (cdr c)))
                    (list name
                          (vector (propertize name 'face 'bold)
                                  (if (kafka-connected-p name)
                                      (propertize "connected" 'face 'success)
                                    (propertize "-" 'face 'shadow))
                                  (or (plist-get p :environment) "")
                                  (if (plist-get p :read-only) "read-only" "read-write")
                                  (or (plist-get p :bootstrap) "")))))
                kafka-clusters)))

;;;###autoload
(defun kafka ()
  "Open the Kafka cluster list."
  (interactive)
  (with-current-buffer (get-buffer-create "*Kafka Clusters*")
    (kafka-clusters-mode)
    (revert-buffer)
    (pop-to-buffer (current-buffer))))

(defun kafka-clusters--at-point ()
  (or (tabulated-list-get-id) (user-error "No cluster on this line")))

(defun kafka-clusters-connect ()
  "Connect to the cluster at point."
  (interactive)
  (let* ((name (kafka-clusters--at-point))
         (info (kafka-ensure-connected name)))
    (message "Connected to %s: %d brokers, %d topics" name
             (length (plist-get info :brokers)) (plist-get info :topicCount))
    (revert-buffer)))

(defun kafka-clusters-disconnect ()
  "Disconnect the cluster at point."
  (interactive)
  (let ((name (kafka-clusters--at-point)))
    (when (kafka-rpc-live-p)
      (kafka-rpc-request :kafka/disconnect (list :connectionId name)))
    (remhash name kafka--connected)
    (revert-buffer)))

(declare-function kafka-topics "kafka-topics")
(declare-function kafka-groups "kafka-groups")

(defun kafka-clusters-open ()
  "Open the topic list for the cluster at point."
  (interactive)
  (kafka-topics (kafka-clusters--at-point)))

(defun kafka-clusters-groups ()
  "Open the consumer group list for the cluster at point."
  (interactive)
  (kafka-groups (kafka-clusters--at-point)))

(defun kafka-clusters-brokers ()
  "Show brokers for the cluster at point."
  (interactive)
  (kafka-brokers (kafka-clusters--at-point)))

;;;; Brokers

(defvar-local kafka-brokers--info nil)

(defvar-keymap kafka-brokers-mode-map
  :parent tabulated-list-mode-map
  "RET" #'kafka-brokers-configs
  "g" #'revert-buffer)

(define-derived-mode kafka-brokers-mode tabulated-list-mode "Kafka Brokers"
  "Major mode listing brokers of a cluster."
  (setq tabulated-list-format [("Id" 6 kafka--num-sort) ("Host" 40 t) ("Port" 8 t) ("" 0 nil)])
  (add-hook 'tabulated-list-revert-hook #'kafka-brokers--refresh nil t)
  (tabulated-list-init-header))

(defun kafka--num-sort (a b)
  "Sort tabulated rows A and B numerically by first column."
  (< (string-to-number (aref (cadr a) 0)) (string-to-number (aref (cadr b) 0))))

(defun kafka-brokers--refresh ()
  (kafka-ensure-connected kafka-connection)
  (let ((info (kafka-rpc-request :kafka/cluster/describe (list :connectionId kafka-connection))))
    (setq header-line-format (kafka-header-line kafka-connection
                                                (format "controller/origin: %s"
                                                        (plist-get info :origBrokerName))))
    (setq tabulated-list-entries
          (mapcar (lambda (b)
                    (list (plist-get b :id)
                          (vector (number-to-string (plist-get b :id))
                                  (plist-get b :host)
                                  (number-to-string (plist-get b :port))
                                  (if (eql (plist-get b :id) (plist-get info :origBrokerId))
                                      "(responded)" ""))))
                  (plist-get info :brokers)))))

(defun kafka-brokers (name)
  "List brokers for connection NAME."
  (interactive (list (kafka-read-connection)))
  (kafka-ensure-connected name)
  (with-current-buffer (get-buffer-create (format "*Kafka Brokers: %s*" name))
    (kafka-brokers-mode)
    (setq kafka-connection name)
    (revert-buffer)
    (pop-to-buffer (current-buffer))))

(declare-function kafka-config-show "kafka-topics")

(defun kafka-brokers-configs ()
  "Show configuration for the broker at point."
  (interactive)
  (let ((id (or (tabulated-list-get-id) (user-error "No broker here"))))
    (kafka-config-show kafka-connection 'broker id)))

;;;; Capabilities

(defun kafka-capabilities ()
  "Display the bridge's capability/parity table."
  (interactive)
  (let ((caps (kafka-rpc-request :kafka/capabilities nil)))
    (with-current-buffer (get-buffer-create "*Kafka Capabilities*")
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (format "kafka-bridge %s, librdkafka %s\n\n"
                        (plist-get (plist-get kafka-rpc-server-info :serverInfo) :version)
                        (plist-get kafka-rpc-server-info :librdkafkaVersion)))
        (seq-doseq (c caps)
          (insert (format "%-4s %-30s %-12s %s\n"
                          (if (kafka-json-bool (plist-get c :implemented)) "[x]" "[ ]")
                          (plist-get c :id) (plist-get c :state) (plist-get c :note))))
        (special-mode))
      (goto-char (point-min))
      (pop-to-buffer (current-buffer)))))

(require 'kafka-topics)
(require 'kafka-records)
(require 'kafka-producer)
(require 'kafka-groups)
(require 'kafka-acls)
(require 'kafka-cluster-admin)
(require 'kafka-schema-registry)
(require 'kafka-connect)
(require 'kafka-transient)

(provide 'kafka)
;;; kafka.el ends here
