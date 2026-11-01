;;; kafka-schema-registry.el --- Schema Registry browser -*- lexical-binding: t; -*-
;; SPDX-License-Identifier: GPL-3.0-or-later
;;; Commentary:
;; Per-profile :schema-registry settings, with auth-source credentials.
;;; Code:
(require 'kafka-rpc)
(require 'tabulated-list)
(require 'auth-source)
(require 'url-parse)
(require 'json)
(require 'js)
(defvar kafka-connection)
(declare-function kafka--profile "kafka")
(declare-function kafka-read-connection "kafka")
(declare-function kafka-ensure-connected "kafka")
(declare-function kafka-header-line "kafka")
(declare-function kafka-confirm-write "kafka")
(declare-function kafka-confirm-exact "kafka")
(defvar-local kafka-schema-registry--subject nil)

(defun kafka-schema-registry--params (name)
  "Build registry parameters for NAME without retaining credentials."
  (let* ((p (plist-get (kafka--profile name) :schema-registry))
         (url (or (plist-get p :url) (user-error "No Schema Registry configured")))
         (user (plist-get p :basic-auth-user))
         (params (list :connectionId name :url url)))
    (dolist (pair '((:basic-auth-user . :basicAuthUser)
                    (:bearer-token . :bearerToken)
                    (:ssl-ca-location . :sslCaLocation)
                    (:ssl-certificate-location . :sslCertificateLocation)
                    (:ssl-key-location . :sslKeyLocation)))
      (when (plist-get p (car pair))
        (setq params (plist-put params (cdr pair) (plist-get p (car pair))))))
    (when user
      (let* ((entry (car (auth-source-search
                         :host (url-host (url-generic-parse-url url)) :user user :max 1)))
             (secret (plist-get entry :secret)))
        (when secret
          (setq params (plist-put params :basicAuthPassword
                                  (if (functionp secret) (funcall secret) secret))))))
    params))

(defun kafka-schema-registry--configure (name)
  "Configure registry for connected profile NAME."
  (kafka-ensure-connected name)
  (kafka-rpc-request :kafka/registry/configure (kafka-schema-registry--params name)))

(defvar-keymap kafka-schema-registry-mode-map
  :parent tabulated-list-mode-map
  "RET" #'kafka-schema-registry-open
  "D" #'kafka-schema-registry-delete
  "g" #'revert-buffer)
(define-derived-mode kafka-schema-registry-mode tabulated-list-mode "Kafka Schemas"
  "Browse subjects or versions."
  (setq tabulated-list-format [("Subject / version" 60 t)])
  (add-hook 'tabulated-list-revert-hook #'kafka-schema-registry--refresh nil t)
  (tabulated-list-init-header))
(defun kafka-schema-registry--refresh ()
  (kafka-schema-registry--configure kafka-connection)
  (let ((items (kafka-rpc-request
                (if kafka-schema-registry--subject :kafka/registry/versions :kafka/registry/subjects)
                (append (list :connectionId kafka-connection)
                        (when kafka-schema-registry--subject
                          (list :subject kafka-schema-registry--subject))))))
    (setq tabulated-list-entries
          (mapcar (lambda (x) (list x (vector (format "%s" x)))) items))
    (setq header-line-format
          (kafka-header-line kafka-connection (or kafka-schema-registry--subject "Schema Registry")))))

;;;###autoload
(defun kafka-schema-registry (name)
  "Browse Schema Registry subjects for connection NAME."
  (interactive (list (kafka-read-connection)))
  (kafka-schema-registry--configure name)
  (with-current-buffer (get-buffer-create (format "*Kafka Schemas: %s*" name))
    (kafka-schema-registry-mode)
    (setq kafka-connection name)
    (revert-buffer)
    (pop-to-buffer (current-buffer))))

(defun kafka-schema-registry-open ()
  "Open versions or schema at point."
  (interactive)
  (let ((id (or (tabulated-list-get-id) (user-error "No schema here")))
        (name kafka-connection)
        (subject kafka-schema-registry--subject))
    (if subject
        (let ((schema (kafka-rpc-request :kafka/registry/schema
                                        (list :connectionId name :subject subject :version id))))
          (with-current-buffer (get-buffer-create (format "*Kafka Schema: %s/%s/%s*" name subject id))
            (let ((inhibit-read-only t))
              (erase-buffer)
              (insert (plist-get schema :schema))
              (if (equal (plist-get schema :schemaType) "PROTOBUF")
                  (fundamental-mode)
                (js-json-mode)
                (json-pretty-print-buffer))
              (setq-local kafka-connection name)
              (setq-local kafka-schema-registry--subject subject)
              (setq header-line-format (kafka-header-line name subject (format "schema id %s" (plist-get schema :id))))
              (goto-char (point-min))
              (read-only-mode 1))
            (pop-to-buffer (current-buffer))))
      (with-current-buffer (get-buffer-create (format "*Kafka Schema Versions: %s/%s*" name id))
        (kafka-schema-registry-mode)
        (setq kafka-connection name kafka-schema-registry--subject id)
        (revert-buffer)
        (pop-to-buffer (current-buffer))))))

(defun kafka-schema-registry-register (name subject type)
  "Register current buffer as TYPE schema for SUBJECT on NAME."
  (interactive (list (kafka-read-connection) (read-string "Subject: ")
                     (completing-read "Schema type: " '("AVRO" "JSON" "PROTOBUF") nil t)))
  (kafka-confirm-write name (format "Register schema for %s" subject))
  (let ((schema (buffer-substring-no-properties (point-min) (point-max))))
    (kafka-schema-registry--configure name)
    (message "Registered: %s" (kafka-rpc-request :kafka/registry/register
                              (list :connectionId name :subject subject :schemaType type :schema schema)))))

(defun kafka-schema-registry-check (name subject type)
  "Check current buffer's TYPE schema compatibility for SUBJECT on NAME."
  (interactive (list (kafka-read-connection) (read-string "Subject: ")
                     (completing-read "Schema type: " '("AVRO" "JSON" "PROTOBUF") nil t)))
  (let ((schema (buffer-substring-no-properties (point-min) (point-max))))
    (kafka-schema-registry--configure name)
    (let ((result (kafka-rpc-request :kafka/registry/compatibility
                                   (list :connectionId name :subject subject :schemaType type :schema schema))))
      (message "Compatibility: %s" (if (eq (plist-get result :is_compatible) t) "compatible" "INCOMPATIBLE"))
      result)))

(defun kafka-schema-registry-delete (&optional permanent)
  "Delete the subject or version at point.  Prefix PERMANENT hard deletes."
  (interactive "P")
  (let* ((id (or (tabulated-list-get-id) (user-error "No schema here")))
         (subject (or kafka-schema-registry--subject id))
         (confirm (kafka-confirm-exact kafka-connection
                                       (format "%s DELETE schema %s" (if permanent "PERMANENT" "Soft") subject)
                                       subject)))
    (kafka-rpc-request :kafka/registry/delete
                      (append (list :connectionId kafka-connection :subject subject :confirmName confirm
                                    :permanent (if permanent t :json-false))
                              (when kafka-schema-registry--subject (list :version id))))
    (revert-buffer)))

(defun kafka-schema-registry-record-detail (name record)
  "Return decoded JSON for RECORD on NAME, or nil if not registry encoded."
  (let* ((value (plist-get record :value))
         (bytes (when value
                  (if (equal (plist-get value :encoding) "base64")
                      (base64-decode-string (plist-get value :data))
                    (encode-coding-string (plist-get value :data) 'utf-8)))))
    (when (and bytes (>= (length bytes) 5) (= (aref bytes 0) 0)
               (plist-get (kafka--profile name) :schema-registry))
      (condition-case err
          (progn
            (kafka-schema-registry--configure name)
            (let ((decoded (kafka-rpc-request :kafka/registry/decode
                                             (list :connectionId name :bytes (base64-encode-string bytes t)))))
              (with-temp-buffer
                (insert (json-encode (plist-get decoded :json)))
                (json-pretty-print-buffer)
                (format "\nDecoded %s (schema id %s):\n%s\n"
                        (plist-get decoded :format) (plist-get decoded :schemaId) (buffer-string)))))
        (error (format "\nSchema decode failed: %s\n" (error-message-string err)))))))

(provide 'kafka-schema-registry)
;;; kafka-schema-registry.el ends here
