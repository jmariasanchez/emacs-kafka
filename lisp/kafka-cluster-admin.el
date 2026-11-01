;;; kafka-cluster-admin.el --- Wire protocol cluster administration -*- lexical-binding: t; -*-

;; Copyright (C) 2026 kafka.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:
;; Reassignments use an editable replica plan with a broker-backed preview.
;; Quotas and share groups are inspected without joining consumer groups.

;;; Code:
(require 'kafka-rpc)
(require 'tabulated-list)
(require 'seq)
(require 'subr-x)
(require 'json)
(defvar kafka-connection)
(declare-function kafka-read-connection "kafka")
(declare-function kafka-ensure-connected "kafka")
(declare-function kafka-header-line "kafka")
(declare-function kafka-confirm-write "kafka")
(declare-function kafka-confirm-exact "kafka")

(defvar-local kafka-admin--kind nil)
(defvar-local kafka-reassignment--topic nil)

(defvar-keymap kafka-cluster-admin-mode-map
  :parent tabulated-list-mode-map
  "g" #'revert-buffer
  "e" #'kafka-admin-edit
  "+" #'kafka-admin-edit
  "D" #'kafka-quota-delete
  "RET" #'kafka-share-group-describe)

(define-derived-mode kafka-cluster-admin-mode tabulated-list-mode "Kafka Admin"
  "Major mode for cluster administration lists."
  (setq tabulated-list-padding 1)
  (add-hook 'tabulated-list-revert-hook #'kafka-admin--refresh nil t))

(defun kafka-admin--request (method &rest params)
  "Call METHOD with PARAMS on this buffer's connection."
  (kafka-ensure-connected kafka-connection)
  (kafka-rpc-request method (append (list :connectionId kafka-connection) params)))

(defun kafka-admin--replicas (replicas)
  "Format REPLICAS for a table or plan."
  (mapconcat #'number-to-string replicas ","))

(defun kafka-admin--refresh ()
  "Refresh the current admin list."
  (setq header-line-format (kafka-header-line kafka-connection (symbol-name kafka-admin--kind)))
  (setq tabulated-list-entries
        (pcase kafka-admin--kind
          ('reassignments
           (mapcar (lambda (p)
                     (list p (vector (plist-get p :topic)
                                     (number-to-string (plist-get p :partition))
                                     (kafka-admin--replicas (plist-get p :replicas))
                                     (kafka-admin--replicas (plist-get p :addingReplicas))
                                     (kafka-admin--replicas (plist-get p :removingReplicas)))))
                   (kafka-admin--request :kafka/reassignments/list)))
          ('log-dirs
           (let (rows)
             (seq-doseq (broker (kafka-admin--request :kafka/logDirs/describe))
               (if (eq (plist-get broker :ok) :json-false)
                   (push (list broker (vector (format "%s" (plist-get broker :brokerId))
                                               (or (plist-get broker :error) "Failed") "" "" "" "")) rows)
                 (seq-doseq (dir (plist-get broker :directories))
                   (seq-doseq (p (plist-get dir :partitions))
                     (push (list p (vector (format "%s" (plist-get broker :brokerId))
                                           (plist-get dir :path) (plist-get p :topic)
                                           (format "%s" (plist-get p :partition))
                                           (format "%s" (plist-get p :size))
                                           (format "%s" (plist-get p :offsetLag)))) rows)))))
             (nreverse rows)))
          ('quotas
           (let (rows)
             (seq-doseq (entry (kafka-admin--request :kafka/quotas/describe))
               (seq-doseq (quota (plist-get entry :values))
                 (push (list (list :entity (plist-get entry :entity) :key (plist-get quota :key))
                             (vector (kafka-quota--label (plist-get entry :entity))
                                     (plist-get quota :key) (format "%s" (plist-get quota :value)))) rows)))
             (nreverse rows)))
          ('share-groups
           (mapcar (lambda (g) (list (plist-get g :groupId)
                                     (vector (plist-get g :groupId) (plist-get g :state)
                                             (format "%s" (plist-get g :brokerId)))))
                   (kafka-admin--request :kafka/shareGroups/list))))))

(defun kafka-admin--open (name kind columns)
  "Open the KIND list for NAME with COLUMNS."
  (kafka-ensure-connected name)
  (with-current-buffer (get-buffer-create (format "*Kafka %s: %s*" kind name))
    (kafka-cluster-admin-mode)
    (setq kafka-connection name kafka-admin--kind kind tabulated-list-format columns)
    (tabulated-list-init-header)
    (revert-buffer)
    (pop-to-buffer (current-buffer))))

;;;###autoload
(defun kafka-reassignments (name)
  "List active reassignments on NAME.  Press e to edit a topic's plan."
  (interactive (list (kafka-read-connection)))
  (kafka-admin--open name 'reassignments
                     [("Topic" 40 t) ("P" 5 t) ("Replicas" 20 nil) ("Adding" 20 nil) ("Removing" 20 nil)]))

;;;###autoload
(defun kafka-log-dirs (name)
  "Show per-broker replica storage on NAME."
  (interactive (list (kafka-read-connection)))
  (kafka-admin--open name 'log-dirs
                     [("Broker" 7 t) ("Directory" 35 t) ("Topic" 35 t) ("P" 5 t) ("Bytes" 14 nil) ("Offset lag" 12 nil)]))

;;;###autoload
(defun kafka-quotas (name)
  "List client quotas on NAME.  Press e to set, D to remove."
  (interactive (list (kafka-read-connection)))
  (kafka-admin--open name 'quotas [("Entity" 48 t) ("Key" 32 t) ("Value" 20 nil)]))

;;;###autoload
(defun kafka-share-groups (name)
  "List share groups on NAME.  RET describes without joining."
  (interactive (list (kafka-read-connection)))
  (kafka-admin--open name 'share-groups [("Group" 50 t) ("State" 20 t) ("Coordinator" 12 t)]))

(defun kafka-admin-edit ()
  "Edit a reassignment or quota from the current admin list."
  (interactive)
  (pcase kafka-admin--kind
    ('reassignments (kafka-reassignment-edit kafka-connection (read-string "Topic: " (plist-get (tabulated-list-get-id) :topic))))
    ('quotas (call-interactively #'kafka-quota-set))
    (_ (user-error "This view has no editable values"))))

(defvar-keymap kafka-reassignment-mode-map
  :parent text-mode-map
  "C-c C-p" #'kafka-reassignment-preview
  "C-c C-c" #'kafka-reassignment-submit)

(define-derived-mode kafka-reassignment-mode text-mode "Kafka Replicas"
  "Edit partition: replica,replica lines.  Use cancel instead of replicas.
C-c C-p previews.  C-c C-c previews again and requests exact confirmation.")

(defun kafka-reassignment-edit (name topic)
  "Open an editable replica plan for TOPIC on NAME."
  (kafka-ensure-connected name)
  (let ((detail (kafka-rpc-request :kafka/topic/describe (list :connectionId name :topic topic))))
    (with-current-buffer (get-buffer-create (format "*Kafka Replica Plan: %s/%s*" name topic))
      (when (and (buffer-modified-p) (> (buffer-size) 0))
        (user-error "Existing plan has unsaved edits, submit or kill it first"))
      (erase-buffer)
      (kafka-reassignment-mode)
      (setq kafka-connection name kafka-reassignment--topic topic)
      (setq header-line-format (kafka-header-line name topic "C-c C-p preview, C-c C-c submit"))
      (insert "# partition: replicas (or cancel)\n")
      (seq-doseq (p (plist-get detail :partitions))
        (insert (format "%d: %s\n" (plist-get p :id) (kafka-admin--replicas (plist-get p :replicas)))))
      (set-buffer-modified-p nil)
      (pop-to-buffer (current-buffer)))))

(defun kafka-reassignment--parse ()
  "Parse the editable plan without evaluating any Lisp."
  (let (parts seen)
    (seq-doseq (line (split-string (buffer-string) "\n" t))
      (setq line (string-trim line))
      (unless (or (string-empty-p line) (string-prefix-p "#" line))
        (unless (string-match "\\`\\([0-9]+\\):[ \t]*\\(cancel\\|[0-9]+\\(?:[ \t]*,[ \t]*[0-9]+\\)*\\)[ \t]*\\'" line)
          (user-error "Invalid replica plan line: %s" line))
        (let* ((partition (string-to-number (match-string 1 line)))
               (text (match-string 2 line))
               (replicas (unless (equal text "cancel") (mapcar #'string-to-number (split-string text ",")))))
          (when (member partition seen) (user-error "Duplicate partition %d" partition))
          (when (/= (length replicas) (length (delete-dups (copy-sequence replicas))))
            (user-error "Duplicate replica in partition %d" partition))
          (push partition seen)
          (push (list :partition partition :replicas (if replicas (vconcat replicas) nil)) parts))))
    (unless parts (user-error "Empty replica plan"))
    (vconcat (nreverse parts))))

(defun kafka-reassignment--preview (parts)
  "Preview PARTS against current broker metadata."
  (let ((result (kafka-admin--request :kafka/reassignments/alter :topic kafka-reassignment--topic :partitions parts :dryRun t)))
    (with-current-buffer (get-buffer-create "*Kafka Replica Preview*")
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert "Replica changes (null target means cancel):\n\n")
        (seq-doseq (change (plist-get result :changes))
          (insert (format "%s/%s\n- %s\n+ %s\n\n" (plist-get change :topic) (plist-get change :partition)
                          (kafka-admin--replicas (plist-get change :currentReplicas))
                          (if (plist-get change :targetReplicas)
                              (kafka-admin--replicas (plist-get change :targetReplicas)) "CANCEL"))))
        (special-mode))
      (display-buffer (current-buffer)))
    result))

(defun kafka-reassignment-preview ()
  "Show a current-vs-target replica diff without making changes."
  (interactive)
  (kafka-reassignment--preview (kafka-reassignment--parse)))

(defun kafka-reassignment-submit ()
  "Preview and submit this plan after exact topic-name confirmation."
  (interactive)
  (let ((parts (kafka-reassignment--parse)))
    (kafka-reassignment--preview parts)
    (let ((confirm (kafka-confirm-exact kafka-connection "SUBMIT replica reassignment" kafka-reassignment--topic)))
      (kafka-admin--request :kafka/reassignments/alter :topic kafka-reassignment--topic :partitions parts :confirmName confirm)
      (set-buffer-modified-p nil)
      (message "Reassignment submitted.  Refresh the reassignment list to monitor it."))))

(defun kafka-quota--label (entity)
  "Format quota ENTITY for display."
  (mapconcat (lambda (e) (format "%s=%s" (plist-get e :entityType) (or (plist-get e :entityName) "<default>"))) entity ", "))

(defun kafka-quota-set (type name key value)
  "Set quota KEY to VALUE for entity TYPE and NAME."
  (interactive
   (list (completing-read "Entity type: " '("user" "client-id" "ip") nil t)
         (read-string "Entity name (empty for default): ")
         (completing-read "Quota: " '("producer_byte_rate" "consumer_byte_rate" "request_percentage" "controller_mutation_rate" "connection_creation_rate") nil t)
         (read-number "Value: ")))
  (unless (and (numberp value) (>= value 0)) (user-error "Quota must be nonnegative"))
  (kafka-confirm-write kafka-connection (format "Set %s quota for %s" key name))
  (let ((params (list :entity (vector (list :entityType type :entityName (unless (string-empty-p name) name)))
                      :ops (vector (list :key key :value value)))))
    (apply #'kafka-admin--request :kafka/quotas/alter (append params (list :validateOnly t)))
    (apply #'kafka-admin--request :kafka/quotas/alter params))
  (revert-buffer))

(defun kafka-quota-delete ()
  "Remove the quota at point with exact entity-name confirmation."
  (interactive)
  (unless (eq kafka-admin--kind 'quotas) (user-error "Not a quota view"))
  (let* ((row (or (tabulated-list-get-id) (user-error "No quota here")))
         (entity (plist-get row :entity))
         (name (mapconcat (lambda (e) (or (plist-get e :entityName) "<default>")) entity "/"))
         (confirm (kafka-confirm-exact kafka-connection "REMOVE quota" name)))
    (kafka-admin--request :kafka/quotas/alter :entity (vconcat entity)
                         :ops (vector (list :key (plist-get row :key) :value nil)) :confirmName confirm)
    (revert-buffer)))

(defun kafka-share-group-describe ()
  "Describe the share group at point and display its offsets."
  (interactive)
  (unless (eq kafka-admin--kind 'share-groups) (user-error "Not a share group view"))
  (let* ((group (or (tabulated-list-get-id) (user-error "No share group here")))
         (detail (kafka-admin--request :kafka/shareGroup/describe :groupId group))
         (offsets (kafka-admin--request :kafka/shareGroup/offsets :groupId group))
         (name kafka-connection))
    (with-current-buffer (get-buffer-create (format "*Kafka Share Group: %s/%s*" name group))
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (json-encode (list :description detail :offsets (vconcat offsets))))
        (json-pretty-print-buffer)
        (special-mode)
        (setq header-line-format (kafka-header-line name group)))
      (pop-to-buffer (current-buffer)))))

(provide 'kafka-cluster-admin)
;;; kafka-cluster-admin.el ends here
