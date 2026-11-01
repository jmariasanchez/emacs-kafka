;;; kafka-transient.el --- Transient menus for kafka.el -*- lexical-binding: t; -*-

;; Copyright (C) 2026 kafka.el contributors
;; SPDX-License-Identifier: GPL-3.0-or-later

;;; Commentary:
;; Transient is bundled with supported Emacs releases.  Optional lane commands
;; are hidden until loaded.  Older bundled Transient validates commands before
;; applying inapt predicates, so these suffixes also need visibility predicates.

;;; Code:
(require 'transient)
(require 'kafka-rpc)
(require 'kafka-producer)
(require 'kafka-workflows)

(defvar kafka-connection)
(declare-function kafka "kafka")
(declare-function kafka-read-connection "kafka")
(declare-function kafka-ensure-connected "kafka")
(declare-function kafka-brokers "kafka")
(declare-function kafka-capabilities "kafka")
(declare-function kafka-acls "kafka-acls")
(declare-function kafka-scram-users "kafka-acls")
(declare-function kafka-cluster-info "kafka-cluster-admin")
(declare-function kafka-group-reset-offsets "kafka-cluster-admin")
(declare-function kafka-delete-records "kafka-cluster-admin")
(declare-function kafka-reassignments "kafka-cluster-admin")
(declare-function kafka-log-dirs "kafka-cluster-admin")
(declare-function kafka-quotas "kafka-cluster-admin")
(declare-function kafka-share-groups "kafka-cluster-admin")
(declare-function kafka-schema-registry "kafka-schema-registry")
(declare-function kafka-connectors "kafka-connect")
(declare-function kafka-streams-apps "kafka-connect")

(defun kafka-transient-connect (name)
  "Connect to the named profile NAME."
  (interactive (list (kafka-read-connection)))
  (kafka-ensure-connected name)
  (message "Connected to %s" name))

(defun kafka-transient-configs (name topic)
  "Show configs for TOPIC on NAME."
  (interactive (list (kafka-read-connection) (read-string "Topic: ")))
  (kafka-config-show name 'topic topic))

(defun kafka-restart-bridge ()
  "Stop the bridge and start a fresh one, reconnecting profiles lazily."
  (interactive)
  (when (yes-or-no-p "Restart Kafka bridge and stop active streams/transactions? ")
    (kafka-rpc-stop)
    (kafka-rpc-connection)
    (message "Kafka bridge restarted")))

(defun kafka-events-log ()
  "Display the existing bridge events log, without enabling payload logging."
  (interactive)
  (if-let* ((buffer (get-buffer "*kafka-bridge events*")))
      (pop-to-buffer buffer)
    (user-error "No events log.  Set kafka-rpc-log-events and restart to enable sensitive RPC logging")))

;;;###autoload
(transient-define-prefix kafka-dispatch (&optional name)
  "Kafka workflows, with optional connection NAME as the default profile."
  [["Clusters"
    ("cl" "Open list" kafka)
    ("cc" "Connect" kafka-transient-connect)
    ("cb" "Brokers" kafka-brokers)
    ("ci" "Cluster info" kafka-cluster-info
     :if (lambda () (fboundp 'kafka-cluster-info))
     :inapt-if-not (lambda () (fboundp 'kafka-cluster-info)))]
   ["Topics"
    ("tl" "List" kafka-topics)
    ("tc" "Create" kafka-topic-create)
    ("td" "Describe" kafka-topic)
    ("tf" "Configs" kafka-transient-configs)
    ("tx" "Delete records" kafka-delete-records
     :if (lambda () (fboundp 'kafka-delete-records))
     :inapt-if-not (lambda () (fboundp 'kafka-delete-records)))]
   ["Records"
    ("rb" "Browse" kafka-records)
    ("rl" "Live tail" kafka-records-live
     :inapt-if-not (lambda () (derived-mode-p 'kafka-records-mode)))
    ("rs" "Seek" kafka-records-seek
     :inapt-if-not (lambda () (derived-mode-p 'kafka-records-mode)))
    ("rf" "Filter" kafka-records-filter
     :inapt-if-not (lambda () (derived-mode-p 'kafka-records-mode)))
    ("rq" "Saved query" kafka-run-saved-query)
    ("r/" "Search" kafka-records-search)]]
  [["Produce"
    ("pb" "Buffer" kafka-produce)
    ("pr" "Region" kafka-produce-region)
    ("pf" "File" kafka-produce-file)
    ("pt" "Transactional region" kafka-produce-transactional)]
   ["Groups"
    ("gl" "List" kafka-groups)
    ("gd" "Describe" kafka-group)
    ("gr" "Reset offsets" kafka-group-reset-offsets
     :if (lambda () (fboundp 'kafka-group-reset-offsets))
     :inapt-if-not (lambda () (fboundp 'kafka-group-reset-offsets)))]
   ["Security"
    ("sa" "ACLs" kafka-acls
     :if (lambda () (fboundp 'kafka-acls))
     :inapt-if-not (lambda () (fboundp 'kafka-acls)))
    ("ss" "SCRAM users" kafka-scram-users
     :if (lambda () (fboundp 'kafka-scram-users))
     :inapt-if-not (lambda () (fboundp 'kafka-scram-users)))
    ("sq" "Quotas" kafka-quotas
     :if (lambda () (fboundp 'kafka-quotas))
     :inapt-if-not (lambda () (fboundp 'kafka-quotas)))]]
  [["Operations"
    ("or" "Reassignments" kafka-reassignments
     :if (lambda () (fboundp 'kafka-reassignments))
     :inapt-if-not (lambda () (fboundp 'kafka-reassignments)))
    ("ol" "Log dirs" kafka-log-dirs
     :if (lambda () (fboundp 'kafka-log-dirs))
     :inapt-if-not (lambda () (fboundp 'kafka-log-dirs)))
    ("og" "Share groups" kafka-share-groups
     :if (lambda () (fboundp 'kafka-share-groups))
     :inapt-if-not (lambda () (fboundp 'kafka-share-groups)))]
   ["Ecosystem"
    ("es" "Schema Registry" kafka-schema-registry
     :if (lambda () (fboundp 'kafka-schema-registry))
     :inapt-if-not (lambda () (fboundp 'kafka-schema-registry)))
    ("ec" "Connectors" kafka-connectors
     :if (lambda () (fboundp 'kafka-connectors))
     :inapt-if-not (lambda () (fboundp 'kafka-connectors)))
    ("ea" "Streams apps" kafka-streams-apps
     :if (lambda () (fboundp 'kafka-streams-apps))
     :inapt-if-not (lambda () (fboundp 'kafka-streams-apps)))]
   ["Bridge"
    ("bc" "Capabilities" kafka-capabilities)
    ("br" "Restart" kafka-restart-bridge)
    ("be" "Events log" kafka-events-log)]]
  (interactive (list (kafka-read-connection)))
  (when name (setq-local kafka-connection name))
  (transient-setup 'kafka-dispatch))

(transient-define-prefix kafka-records-dispatch ()
  "Record buffer operations."
  [["Read" ("g" "Fetch" kafka-records-fetch) ("l" "Live" kafka-records-live)
    ("k" "Stop" kafka-records-stop) ("s" "Seek" kafka-records-seek) ("/" "Filter" kafka-records-filter)]
   ["Workflows" ("m" "Mark" kafka-records-mark) ("=" "Diff" kafka-records-diff)
    ("S" "Search" kafka-records-search) ("q" "Saved query" kafka-run-saved-query)
    ("b" "Bookmark" bookmark-set)]
   ["Record" ("RET" "Show" kafka-records-show) ("w" "Copy" kafka-records-copy-value)
    ("P" "Produce again" kafka-records-reproduce)]])

(transient-define-prefix kafka-topics-dispatch ()
  "Topic list operations."
  [["Topics" ("RET" "Describe" kafka-topics-describe) ("r" "Records" kafka-topics-records)
    ("p" "Produce" kafka-topics-produce) ("C" "Configs" kafka-topics-configs)]
   ["Manage" ("+" "Create" kafka-topic-create) ("D" "Delete" kafka-topics-delete)
    ("/" "Filter" kafka-topics-filter) ("i" "Internal" kafka-topics-toggle-internal)
    ("b" "Bookmark" bookmark-set)]])

(transient-define-prefix kafka-groups-dispatch ()
  "Consumer group list operations."
  [["Groups" ("RET" "Describe" kafka-groups-describe) ("D" "Delete" kafka-groups-delete)
    ("r" "Reset offsets" kafka-group-reset-offsets
     :if (lambda () (fboundp 'kafka-group-reset-offsets))
     :inapt-if-not (lambda () (fboundp 'kafka-group-reset-offsets)))
    ("b" "Bookmark" bookmark-set)]])

(with-eval-after-load 'kafka-records
  (keymap-set kafka-records-mode-map "?" #'kafka-records-dispatch))
(with-eval-after-load 'kafka-topics
  (keymap-set kafka-topics-mode-map "?" #'kafka-topics-dispatch))
(with-eval-after-load 'kafka-groups
  (keymap-set kafka-groups-mode-map "?" #'kafka-groups-dispatch))

(provide 'kafka-transient)
;;; kafka-transient.el ends here
