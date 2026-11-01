;;; kafka-acls.el --- ACLs, SCRAM users and cluster information -*- lexical-binding: t; -*-
;; Copyright (C) 2026 kafka.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later
;;; Commentary:
;; Administrative views.  Destructive actions require exact confirmation.
;;; Code:
(require 'kafka-rpc)
(require 'tabulated-list)
(require 'pp)
(require 'cl-lib)
(defvar kafka-connection)
(declare-function kafka-read-connection "kafka")
(declare-function kafka-ensure-connected "kafka")
(declare-function kafka-header-line "kafka")
(declare-function kafka-confirm-exact "kafka")
(declare-function kafka-confirm-write "kafka")

(defvar-keymap kafka-acls-mode-map
  :parent tabulated-list-mode-map
  "+" #'kafka-acls-create
  "D" #'kafka-acls-delete
  "g" #'revert-buffer)
(define-derived-mode kafka-acls-mode tabulated-list-mode "Kafka ACLs"
  "List Kafka ACL bindings.  + creates and D deletes the selected binding."
  (setq tabulated-list-format [("Type" 12 t) ("Resource" 30 t) ("Pattern" 10 t)
                               ("Principal" 24 t) ("Host" 16 t) ("Operation" 20 t) ("Permission" 10 t)])
  (setq tabulated-list-padding 1)
  (add-hook 'tabulated-list-revert-hook #'kafka-acls--refresh nil t)
  (tabulated-list-init-header))
(defun kafka-acls--refresh ()
  (kafka-ensure-connected kafka-connection)
  (let ((acls (kafka-rpc-request :kafka/acls/describe (list :connectionId kafka-connection))))
    (setq header-line-format (kafka-header-line kafka-connection (format "%d ACLs" (length acls))))
    (setq tabulated-list-entries
          (mapcar (lambda (a)
                    (list a (vconcat (mapcar (lambda (k) (or (plist-get a k) ""))
                                             '(:resourceType :name :patternType :principal :host :operation :permission)))))
                  acls))))
;;;###autoload
(defun kafka-acls (name)
  "List ACLs on connection NAME."
  (interactive (list (kafka-read-connection)))
  (kafka-ensure-connected name)
  (with-current-buffer (get-buffer-create (format "*Kafka ACLs: %s*" name))
    (kafka-acls-mode)
    (setq kafka-connection name)
    (revert-buffer)
    (pop-to-buffer (current-buffer))))
(defun kafka-acls-create ()
  "Create an ACL after checking the connection write policy."
  (interactive)
  (kafka-confirm-write kafka-connection "create an ACL")
  (let ((acl (list :resourceType (completing-read "Resource type: " '("topic" "group" "cluster" "transactional-id") nil t)
                   :name (read-string "Resource name: ")
                   :patternType (completing-read "Pattern: " '("literal" "prefixed") nil t nil nil "literal")
                   :principal (read-string "Principal: " "User:")
                   :host (read-string "Host: " "*")
                   :operation (completing-read "Operation: " '("read" "write" "describe" "create" "delete" "alter" "describe-configs" "alter-configs" "cluster-action" "idempotent-write" "all") nil t)
                   :permission (completing-read "Permission: " '("allow" "deny") nil t nil nil "allow"))))
    (kafka-rpc-request :kafka/acls/create (list :connectionId kafka-connection :acl acl))
    (revert-buffer)))
(defun kafka-acls-delete ()
  "Delete the exact ACL binding at point."
  (interactive)
  (let* ((acl (or (tabulated-list-get-id) (user-error "No ACL here")))
         (name (plist-get acl :name))
         (confirm (kafka-confirm-exact kafka-connection (format "DELETE ACL %S" acl) name)))
    (kafka-rpc-request :kafka/acls/delete (list :connectionId kafka-connection :filter acl :confirmName confirm))
    (revert-buffer)))

(defvar-keymap kafka-scram-users-mode-map
  :parent tabulated-list-mode-map
  "+" #'kafka-scram-users-upsert
  "D" #'kafka-scram-users-delete
  "g" #'revert-buffer)
(define-derived-mode kafka-scram-users-mode tabulated-list-mode "Kafka SCRAM"
  "List SCRAM credentials without exposing passwords."
  (setq tabulated-list-format [("User" 40 t) ("Mechanism" 20 t) ("Iterations" 12 t)])
  (setq tabulated-list-padding 1)
  (add-hook 'tabulated-list-revert-hook #'kafka-scram-users--refresh nil t)
  (tabulated-list-init-header))
(defun kafka-scram-users--refresh ()
  (kafka-ensure-connected kafka-connection)
  (let ((users (kafka-rpc-request :kafka/scram/describe (list :connectionId kafka-connection))))
    (setq header-line-format (kafka-header-line kafka-connection "SCRAM credentials"))
    (setq tabulated-list-entries
          (apply #'append
                 (mapcar (lambda (u)
                           (mapcar (lambda (c)
                                     (list (list :user (plist-get u :user) :mechanism (plist-get c :mechanism))
                                           (vector (plist-get u :user) (plist-get c :mechanism)
                                                   (number-to-string (plist-get c :iterations)))))
                                   (plist-get u :credentials))) users)))))
;;;###autoload
(defun kafka-scram-users (name)
  "List SCRAM users on NAME."
  (interactive (list (kafka-read-connection)))
  (kafka-ensure-connected name)
  (with-current-buffer (get-buffer-create (format "*Kafka SCRAM: %s*" name))
    (kafka-scram-users-mode)
    (setq kafka-connection name)
    (revert-buffer)
    (pop-to-buffer (current-buffer))))
(defun kafka-scram-users-upsert ()
  "Create or replace a SCRAM credential.  Never retain the password in a buffer."
  (interactive)
  (kafka-confirm-write kafka-connection "create or replace a SCRAM credential")
  (let* ((user (read-string "User: "))
         (mechanism (completing-read "Mechanism: " '("SCRAM-SHA-256" "SCRAM-SHA-512") nil t))
         (password (read-passwd "New password: " t)))
    (unwind-protect
        ;; jsonrpc's opt-in traffic log otherwise records outgoing passwords.
        ;; Suppression is scoped to this synchronous request, on Emacs 29+.
        (cl-letf (((symbol-function 'jsonrpc--log-event) #'ignore))
          (kafka-rpc-request :kafka/scram/alter
                             (list :connectionId kafka-connection
                                   :change (list :operation "upsert" :user user :mechanism mechanism :password password))))
      (clear-string password))
    (revert-buffer)))
(defun kafka-scram-users-delete ()
  "Delete the SCRAM credential at point with an exact username confirmation."
  (interactive)
  (let* ((entry (or (tabulated-list-get-id) (user-error "No credential here")))
         (user (plist-get entry :user))
         (confirm (kafka-confirm-exact kafka-connection (format "DELETE %s credential for %s" (plist-get entry :mechanism) user) user)))
    (kafka-rpc-request :kafka/scram/alter
                       (list :connectionId kafka-connection :change
                             (append (list :operation "delete" :confirmName confirm) entry)))
    (revert-buffer)))

(defun kafka-admin--show (name title value)
  "Show administrative VALUE under TITLE for connection NAME."
  (with-current-buffer (get-buffer-create (format "*Kafka %s: %s*" title name))
    (let ((inhibit-read-only t))
      (erase-buffer)
      (insert (pp-to-string value))
      (special-mode)
      (setq kafka-connection name)
      (setq header-line-format (kafka-header-line name title)))
    (pop-to-buffer (current-buffer))))
;;;###autoload
(defun kafka-cluster-info (name)
  "Show cluster id, controller, nodes and authorized operations on NAME."
  (interactive (list (kafka-read-connection)))
  (kafka-ensure-connected name)
  (kafka-admin--show name "Cluster Info" (kafka-rpc-request :kafka/cluster/info (list :connectionId name))))
(provide 'kafka-acls)
;;; kafka-acls.el ends here
