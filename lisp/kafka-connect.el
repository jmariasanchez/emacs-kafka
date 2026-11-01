;;; kafka-connect.el --- Connect and Streams observability -*- lexical-binding: t; -*-
;; SPDX-License-Identifier: GPL-3.0-or-later
;;; Commentary:
;; Configure profiles with :connect (:url "http://localhost:18083"
;; :basic-auth-user "user"). Passwords are resolved through auth-source.
;;; Code:
(require 'kafka-rpc)
(require 'tabulated-list)
(require 'json)
(require 'auth-source)
(require 'url-parse)
(require 'seq)
(defvar kafka-connection)
(declare-function kafka--profile "kafka")
(declare-function kafka-read-connection "kafka")
(declare-function kafka-ensure-connected "kafka")
(declare-function kafka-header-line "kafka")
(declare-function kafka-confirm-write "kafka")
(declare-function kafka-confirm-exact "kafka")
(defvar-local kafka-connect--name nil)

(defun kafka-connect--configure (name)
  "Configure the Connect endpoint for NAME without sending credentials to logs."
  (kafka-ensure-connected name)
  (let* ((p (plist-get (kafka--profile name) :connect))
         (url (or (plist-get p :url) (user-error "No :connect :url in profile %s" name)))
         (user (plist-get p :basic-auth-user))
         (secret (when user
                   (plist-get (car (auth-source-search
                                    :host (url-host (url-generic-parse-url url))
                                    :user user :max 1)) :secret)))
         (params (list :connectionId name :url url)))
    (when user
      (setq params (append params (list :basicAuthUser user
                                       :basicAuthPassword (if (functionp secret) (funcall secret) secret)))))
    (when (plist-get p :ssl-ca-location)
      (setq params (append params (list :sslCaLocation (plist-get p :ssl-ca-location)))))
    (kafka-rpc-request :kafka/connect/configure params)))

(defun kafka-connect--request (action &rest args)
  "Call Connect ACTION with ARGS on the current connection."
  (kafka-rpc-request (intern (concat ":kafka/connect/" action))
                     (append (list :connectionId kafka-connection) args)))

(defun kafka-connect--row (name item)
  "Build a connector row for NAME and expanded ITEM."
  (let* ((status (plist-get item :status))
         (connector (plist-get status :connector))
         (tasks (plist-get status :tasks)))
    (list name (vector name (or (plist-get status :type) "")
                       (or (plist-get connector :state) "")
                       (format "%d/%d" (seq-count (lambda (x) (equal (plist-get x :state) "RUNNING")) tasks)
                               (seq-count (lambda (x) (equal (plist-get x :state) "FAILED")) tasks))
                       (or (plist-get connector :worker_id) "")))))

(defvar-keymap kafka-connectors-mode-map
  :parent tabulated-list-mode-map
  "RET" #'kafka-connect-detail "g" #'revert-buffer
  "+" #'kafka-connect-create "p" #'kafka-connect-pause
  "r" #'kafka-connect-resume "R" #'kafka-connect-restart
  "s" #'kafka-connect-stop "D" #'kafka-connect-delete
  "t" #'kafka-connect-restart-task "o" #'kafka-connect-offsets
  "P" #'kafka-connect-plugins)
(define-derived-mode kafka-connectors-mode tabulated-list-mode "Kafka Connect"
  "List Kafka Connect connectors."
  (setq tabulated-list-format [("Name" 35 t) ("Type" 10 t) ("State" 12 t)
                               ("Running/Failed" 15 t) ("Worker" 28 t)])
  (add-hook 'tabulated-list-revert-hook #'kafka-connect--refresh nil t)
  (tabulated-list-init-header))
(defun kafka-connect--refresh ()
  "Refresh the connectors list."
  (kafka-connect--configure kafka-connection)
  (let ((items (kafka-connect--request "connectors")) rows)
    (while items
      (push (kafka-connect--row (substring (symbol-name (pop items)) 1) (pop items)) rows))
    (setq tabulated-list-entries (nreverse rows)
          header-line-format (kafka-header-line kafka-connection "Connect"))))
;;;###autoload
(defun kafka-connectors (name)
  "List connectors for connection NAME."
  (interactive (list (kafka-read-connection)))
  (with-current-buffer (get-buffer-create (format "*Kafka Connect: %s*" name))
    (kafka-connectors-mode)
    (setq kafka-connection name)
    (revert-buffer)
    (pop-to-buffer (current-buffer))))
(defun kafka-connect--selected ()
  "Return the connector at point or in the detail buffer."
  (or kafka-connect--name (tabulated-list-get-id) (user-error "No connector selected")))
(defun kafka-connect--show (title value)
  "Show VALUE as JSON under TITLE."
  (with-current-buffer (get-buffer-create (format "*Kafka %s*" title))
    (let ((inhibit-read-only t))
      (erase-buffer) (insert (json-encode value)) (json-pretty-print-buffer))
    (special-mode) (pop-to-buffer (current-buffer))))
(defun kafka-connect-detail ()
  "Open connector configuration, with status and tasks in a separate buffer."
  (interactive)
  (let* ((name (kafka-connect--selected))
         (connection kafka-connection)
         (detail (kafka-connect--request "connector" :connector name)))
    (kafka-connect--show (concat "Connector status: " name)
                         (list :status (plist-get detail :status) :tasks (plist-get detail :tasks)))
    (kafka-connect--edit connection name (plist-get detail :config))))
(defvar-keymap kafka-connect-config-mode-map
  :parent text-mode-map "C-c C-c" #'kafka-connect-save)
(define-derived-mode kafka-connect-config-mode text-mode "Connect Config"
  "Edit connector JSON.  C-c C-c validates then saves.")
(defun kafka-connect--edit (connection name config)
  "Edit CONFIG for NAME on CONNECTION."
  (with-current-buffer (get-buffer-create (format "*Kafka Connector: %s/%s*" connection name))
    (when (and (buffer-modified-p) (> (buffer-size) 0)
               (not (yes-or-no-p "Discard unsaved connector configuration? ")))
      (user-error "Keeping unsaved configuration"))
    (kafka-connect-config-mode)
    (setq kafka-connection connection kafka-connect--name name)
    (erase-buffer) (insert (json-encode config)) (json-pretty-print-buffer)
    (setq header-line-format (kafka-header-line connection name "C-c C-c validate and save"))
    (set-buffer-modified-p nil) (pop-to-buffer (current-buffer))))
(defun kafka-connect-create (name)
  "Create a connector called NAME by editing its JSON configuration."
  (interactive (list (read-string "New connector name: ")))
  (kafka-confirm-write kafka-connection "Create connector")
  (kafka-connect--edit kafka-connection name
                       (list :name name :connector.class "org.apache.kafka.connect.file.FileStreamSourceConnector"
                             :tasks.max "1" :file "" :topic "")))
(defun kafka-connect-save ()
  "Validate edited JSON, show errors, then upsert the connector."
  (interactive)
  (kafka-confirm-write kafka-connection "Save connector configuration")
  (let* ((config (save-excursion
                   (goto-char (point-min))
                   (json-parse-buffer :object-type 'plist :array-type 'list
                                      :null-object nil :false-object :json-false)))
         (validation (kafka-connect--request "validate" :class (plist-get config :connector.class) :config config)))
    (if (> (or (plist-get validation :error_count) 0) 0)
        (progn (kafka-connect--show "Connect validation errors" validation)
               (user-error "Connector validation failed, nothing saved"))
      (kafka-connect--request "upsert" :connector kafka-connect--name :config config)
      (set-buffer-modified-p nil) (message "Connector saved"))))
(defun kafka-connect--action (action &rest args)
  "Confirm and perform connector ACTION with ARGS."
  (let ((name (kafka-connect--selected)))
    (kafka-confirm-write kafka-connection (concat action " connector " name))
    (unless (yes-or-no-p (format "%s connector %s? " action name)) (user-error "Cancelled"))
    (apply #'kafka-connect--request action :connector name args)
    (when (derived-mode-p 'kafka-connectors-mode) (revert-buffer))))
(defun kafka-connect-pause () "Pause the selected connector." (interactive) (kafka-connect--action "pause"))
(defun kafka-connect-resume () "Resume the selected connector." (interactive) (kafka-connect--action "resume"))
(defun kafka-connect-stop () "Stop the selected connector." (interactive) (kafka-connect--action "stop"))
(defun kafka-connect-restart () "Restart the connector and its tasks." (interactive) (kafka-connect--action "restart" :includeTasks t :onlyFailed :json-false))
(defun kafka-connect-restart-task (task)
  "Restart selected connector TASK."
  (interactive (list (read-number "Task id: " 0)))
  (kafka-connect--action "restartTask" :task task))
(defun kafka-connect-delete ()
  "Delete the selected connector after exact confirmation."
  (interactive)
  (let* ((name (kafka-connect--selected))
         (confirm (kafka-confirm-exact kafka-connection "DELETE connector" name)))
    (kafka-connect--request "delete" :connector name :confirmName confirm)
    (revert-buffer)))
(defun kafka-connect-offsets ()
  "View the selected connector offsets."
  (interactive)
  (kafka-connect--show "Connect offsets" (kafka-connect--request "offsets" :connector (kafka-connect--selected))))
(defun kafka-connect-plugins ()
  "View available Connect plugins."
  (interactive)
  (kafka-connect--show "Connect plugins" (kafka-connect--request "plugins")))

(define-derived-mode kafka-streams-mode tabulated-list-mode "Kafka Streams"
  "Observe Streams applications without running a Streams runtime."
  (setq tabulated-list-format [("Application" 40 t) ("State" 14 t) ("Members" 10 t)
                               ("Total lag" 12 t) ("Internal topics" 60 t)])
  (add-hook 'tabulated-list-revert-hook #'kafka-streams--refresh nil t)
  (tabulated-list-init-header))
(defun kafka-streams--refresh ()
  "Refresh Streams applications."
  (kafka-ensure-connected kafka-connection)
  (setq header-line-format (kafka-header-line kafka-connection "Streams observation")
        tabulated-list-entries
        (mapcar (lambda (app)
                  (list (plist-get app :appId)
                        (vector (plist-get app :appId) (plist-get app :state)
                                (number-to-string (plist-get app :members))
                                (number-to-string (plist-get app :totalLag))
                                (mapconcat #'identity (plist-get app :internalTopics) ", "))))
                (kafka-rpc-request :kafka/streams/apps (list :connectionId kafka-connection)))))
;;;###autoload
(defun kafka-streams-apps (name)
  "List Streams applications on NAME using consumer groups and internal topics."
  (interactive (list (kafka-read-connection)))
  (with-current-buffer (get-buffer-create (format "*Kafka Streams: %s*" name))
    (kafka-streams-mode) (setq kafka-connection name)
    (revert-buffer) (pop-to-buffer (current-buffer))))
(provide 'kafka-connect)
;;; kafka-connect.el ends here
