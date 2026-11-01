;;; kafka-rpc.el --- JSON-RPC transport to kafka-bridge  -*- lexical-binding: t; -*-

;; Copyright (C) 2026 kafka.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:

;; Owns the single `kafka-bridge' subprocess and the `jsonrpc' connection to
;; it.  All other modules talk to Kafka exclusively through this file.
;;
;; Notifications from the bridge are routed to handlers registered with
;; `kafka-rpc-on-notification', keyed by method symbol.

;;; Code:

(require 'jsonrpc)
(require 'cl-lib)

(defgroup kafka nil
  "Apache Kafka integration."
  :group 'tools
  :prefix "kafka-")

(defcustom kafka-bridge-program
  (or (getenv "KAFKA_EL_BRIDGE") "kafka-bridge")
  "Path to the `kafka-bridge' executable."
  :type 'string)

(defcustom kafka-bridge-arguments nil
  "Extra command line arguments for `kafka-bridge'."
  :type '(repeat string))

(defcustom kafka-rpc-timeout 30
  "Seconds to wait for synchronous bridge requests."
  :type 'integer)

(defcustom kafka-rpc-log-events nil
  "When non-nil, keep a `*kafka-bridge events*' log of all RPC traffic.
Credentials are never sent back by the bridge, but outgoing
`kafka/connect' requests contain secrets, so leave this off unless
debugging."
  :type 'boolean)

(defvar kafka-rpc--connection nil
  "The live `jsonrpc-process-connection' to the bridge, or nil.")

(defvar kafka-rpc--notification-handlers (make-hash-table :test #'eq)
  "Map of notification method symbol to list of handler functions.")

(defvar kafka-rpc-server-info nil
  "Plist returned by `kafka/initialize'.")

(defun kafka-rpc-on-notification (method fn)
  "Call FN with params plist whenever notification METHOD arrives.
METHOD is a symbol such as `kafka/consume/records'."
  (cl-pushnew fn (gethash method kafka-rpc--notification-handlers)))

(defun kafka-rpc--dispatch-notification (_conn method params)
  "Route notification METHOD with PARAMS to registered handlers."
  (dolist (fn (gethash method kafka-rpc--notification-handlers))
    (condition-case err
        (funcall fn params)
      (error (message "kafka: handler for %s failed: %S" method err)))))

(defun kafka-rpc--on-shutdown (_conn)
  "Clean up after the bridge process exits."
  (setq kafka-rpc--connection nil)
  (run-hooks 'kafka-rpc-shutdown-hook))

(defvar kafka-rpc-shutdown-hook nil
  "Hook run after the bridge process exits, for any reason.")

(defun kafka-rpc-live-p ()
  "Return non-nil if the bridge is running."
  (and kafka-rpc--connection (jsonrpc-running-p kafka-rpc--connection)))

(defun kafka-rpc--events-args ()
  "Initargs controlling the jsonrpc events buffer, for any jsonrpc version.
jsonrpc 1.0.24+ uses `:events-buffer-config'.  Older versions, such as the
1.0.16 bundled with Emacs 29, only know `:events-buffer-scrollback-size'."
  (let ((size (if kafka-rpc-log-events 2000000 0)))
    (if (slot-exists-p 'jsonrpc-connection '-events-buffer-config)
        (list :events-buffer-config (list :size size))
      (list :events-buffer-scrollback-size size))))

(defun kafka-rpc--start ()
  "Start the bridge process and perform the initialize handshake."
  (unless (executable-find kafka-bridge-program)
    (user-error "Cannot find kafka-bridge executable %S; set `kafka-bridge-program'"
                kafka-bridge-program))
  (let* ((process-connection-type nil)
         (proc (make-process
                :name "kafka-bridge"
                :command (cons kafka-bridge-program kafka-bridge-arguments)
                :connection-type 'pipe
                :coding 'utf-8-emacs-unix
                :noquery t
                :stderr (get-buffer-create " *kafka-bridge stderr*")))
         (conn (apply
                #'make-instance 'jsonrpc-process-connection
                :name "kafka-bridge"
                :process proc
                :notification-dispatcher #'kafka-rpc--dispatch-notification
                :request-dispatcher (lambda (&rest _) (jsonrpc-error "No client methods"))
                :on-shutdown #'kafka-rpc--on-shutdown
                (kafka-rpc--events-args))))
    (setq kafka-rpc--connection conn)
    (setq kafka-rpc-server-info
          (jsonrpc-request conn :kafka/initialize nil :timeout kafka-rpc-timeout))
    conn))

(defun kafka-rpc-connection ()
  "Return the bridge connection, starting the bridge if needed."
  (if (kafka-rpc-live-p) kafka-rpc--connection (kafka-rpc--start)))

(defun kafka-rpc-stop ()
  "Shut down the bridge process."
  (interactive)
  (when (kafka-rpc-live-p)
    (ignore-errors (jsonrpc-request kafka-rpc--connection :kafka/shutdown nil :timeout 3))
    (ignore-errors (jsonrpc-shutdown kafka-rpc--connection)))
  (setq kafka-rpc--connection nil))

(defun kafka-rpc-error-message (err)
  "Return a human-readable message for a `jsonrpc-error' ERR."
  (let ((data (cdr err)))
    (or (alist-get 'jsonrpc-error-message data)
        (format "%S" err))))

(defun kafka-rpc-error-code (err)
  "Return the numeric error code of `jsonrpc-error' ERR."
  (alist-get 'jsonrpc-error-code (cdr err)))

(defun kafka-rpc-clean-params (params)
  "Drop keys whose value is nil from plist PARAMS.
Emacs serializes nil as an empty JSON object, which is never what we
mean for an absent optional field.  Use `:json-false' for false."
  (cl-loop for (k v) on params by #'cddr
           unless (null v) append (list k v)))

(defun kafka-rpc--drain ()
  "Parse messages left buffered after a synchronous request returned.
In jsonrpc 1.0.16 (Emacs 29) the reply to `jsonrpc-request' exits the
process filter non-locally, so notifications that arrived in the same
chunk wait until more output comes.  A quiet live stream would then
never show its records.  Re-running the filter with no new input
processes them.  Harmless on newer jsonrpc versions."
  (when-let* ((conn kafka-rpc--connection)
              (proc (jsonrpc--process conn))
              ((process-live-p proc))
              ((fboundp 'jsonrpc--process-filter)))
    (run-at-time 0 nil (lambda ()
                         (when (process-live-p proc)
                           (jsonrpc--process-filter proc ""))))))

(defun kafka-rpc-request (method params &optional timeout)
  "Synchronously call METHOD with PARAMS.  Signal `user-error' on failure.
The UI stays responsive to input while waiting; `C-g' cancels."
  (unwind-protect
      (condition-case err
          (jsonrpc-request (kafka-rpc-connection) method (kafka-rpc-clean-params params)
                           :timeout (or timeout kafka-rpc-timeout)
                           :cancel-on-input nil)
        (jsonrpc-error
         (signal 'kafka-error (list (kafka-rpc-error-message err)
                                    (kafka-rpc-error-code err)
                                    (alist-get 'jsonrpc-error-data (cdr err))))))
    (kafka-rpc--drain)))

(defun kafka-rpc-async (method params on-success &optional on-error)
  "Asynchronously call METHOD with PARAMS.
ON-SUCCESS receives the result.  ON-ERROR receives (MESSAGE CODE DATA);
by default the message is displayed."
  (jsonrpc-async-request
   (kafka-rpc-connection) method (kafka-rpc-clean-params params)
   :success-fn on-success
   :error-fn (lambda (e)
               (let ((msg (plist-get e :message))
                     (code (plist-get e :code))
                     (data (plist-get e :data)))
                 (if on-error
                     (funcall on-error msg code data)
                   (message "kafka: %s" msg))))
   :timeout-fn (lambda () (message "kafka: %s timed out" method))
   :timeout kafka-rpc-timeout))

(defun kafka-rpc-notify (method params)
  "Send notification METHOD with PARAMS."
  (jsonrpc-notify (kafka-rpc-connection) method params))

(define-error 'kafka-error "Kafka error" 'user-error)

(provide 'kafka-rpc)
;;; kafka-rpc.el ends here
