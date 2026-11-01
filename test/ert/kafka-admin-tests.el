;;; kafka-admin-tests.el --- Native admin UI tests -*- lexical-binding: t; -*-
;;; Code:
(require 'kafka-tests)
(require 'kafka-acls)
(require 'kafka-groups)

(ert-deftest kafka-admin-selection-validation ()
  (should (equal (kafka-group-reset--selection "orders") '(:topic "orders")))
  (should (equal (kafka-group-reset--selection "orders:0,2") '(:topic "orders" :partitions [0 2])))
  (dolist (bad '("" ":0" "t:-1" "t:a" "t:0:1" "t:0,"))
    (should-error (kafka-group-reset--selection bad) :type 'user-error)))

(ert-deftest kafka-admin-reset-confirmation-before-write ()
  (let ((kafka-clusters '(("c" :bootstrap "unused"))) (sent nil))
    (with-temp-buffer
      (kafka-group-reset-mode)
      (setq kafka-connection "c" kafka-group-reset--params '(:connectionId "c" :groupId "g" :dryRun t))
      (cl-letf (((symbol-function 'read-string) (lambda (&rest _) "wrong"))
                ((symbol-function 'kafka-rpc-request) (lambda (&rest _) (setq sent t))))
        (should-error (kafka-group-reset-apply) :type 'user-error)
        (should-not sent)
        (should kafka-group-reset--params)))))

(ert-deftest kafka-admin-scram-suppresses-traffic-log ()
  (let ((secret (copy-sequence "never-log-me")) (logged nil) (sent nil))
    (with-temp-buffer
      (setq-local kafka-connection "c")
      (cl-letf (((symbol-function 'kafka-confirm-write) #'ignore)
                ((symbol-function 'read-string) (lambda (&rest _) "new-user"))
                ((symbol-function 'completing-read) (lambda (&rest _) "SCRAM-SHA-256"))
                ((symbol-function 'read-passwd) (lambda (&rest _) secret))
                ((symbol-function 'jsonrpc--log-event) (lambda (&rest _) (setq logged t)))
                ((symbol-function 'revert-buffer) #'ignore)
                ((symbol-function 'kafka-rpc-request)
                 (lambda (_method params)
                   (setq sent (equal (plist-get (plist-get params :change) :password) "never-log-me"))
                   (jsonrpc--log-event nil nil))))
        (kafka-scram-users-upsert)
        (should sent)
        (should-not logged)
        (should-not (equal secret "never-log-me"))))))

(ert-deftest kafka-admin-it-cluster-info ()
  (kafka-tests-with-cluster
    (kafka-cluster-info "t-rw")
    (with-current-buffer "*Kafka Cluster Info: t-rw*"
      (should (derived-mode-p 'special-mode))
      (should (string-match-p ":clusterId" (buffer-string)))
      (should (string-match-p ":controller" (buffer-string))))))

(ert-deftest kafka-admin-it-reset-preview-apply ()
  (kafka-tests-with-cluster
    (let ((topic (kafka-tests-topic)) (group (concat (kafka-tests-topic) "-group")))
      (kafka-ensure-connected "t-rw")
      (unwind-protect
          (progn
            (kafka-rpc-request :kafka/topic/create (list :connectionId "t-rw" :topic topic :partitions 1 :replicationFactor 1))
            (dotimes (_ 3)
              (kafka-rpc-request :kafka/produce (list :connectionId "t-rw" :topic topic :partition 0 :value "value")))
            (with-temp-buffer
              (setq-local kafka-group--id group)
              (cl-letf (((symbol-function 'read-string) (lambda (&rest _) topic))
                        ((symbol-function 'completing-read) (lambda (&rest _) "to-latest")))
                (kafka-group-reset-offsets "t-rw")))
            (with-current-buffer (format "*Kafka Reset: t-rw/%s*" group)
              (should (derived-mode-p 'kafka-group-reset-mode))
              (should (= (length tabulated-list-entries) 1))
              (should (equal (aref (cadar tabulated-list-entries) 3) "3"))
              (should (seq-empty-p (kafka-rpc-request :kafka/group/offsets (list :connectionId "t-rw" :groupId group))))
              (cl-letf (((symbol-function 'read-string) (lambda (&rest _) group)))
                (kafka-group-reset-apply))
              (should-not kafka-group-reset--params)
              (should (= (plist-get (seq-elt (kafka-rpc-request :kafka/group/offsets (list :connectionId "t-rw" :groupId group)) 0) :committed) 3))))
        (ignore-errors (kafka-rpc-request :kafka/group/delete (list :connectionId "t-rw" :groupId group :confirmName group)))
        (ignore-errors (kafka-rpc-request :kafka/topic/delete (list :connectionId "t-rw" :topic topic :confirmName topic)))))))

(ert-deftest kafka-admin-it-secure-list-modes ()
  (let ((bootstrap (getenv "KAFKA_EL_TEST_SECURE_BOOTSTRAP")))
    (skip-unless (and bootstrap (executable-find kafka-bridge-program)))
    (let ((kafka-clusters `(("admin-test" :bootstrap ,bootstrap :security-protocol "SASL_PLAINTEXT"
                            :sasl-mechanism "PLAIN" :sasl-username "admin")))
          (kafka-protected-environments nil))
      (cl-letf (((symbol-function 'kafka--secret) (lambda (&rest _) "admin-secret")))
        (unwind-protect
            (progn
              (kafka-acls "admin-test")
              (with-current-buffer "*Kafka ACLs: admin-test*" (should (derived-mode-p 'kafka-acls-mode)))
              (kafka-scram-users "admin-test")
              (with-current-buffer "*Kafka SCRAM: admin-test*"
                (should (derived-mode-p 'kafka-scram-users-mode))
                (should (seq-some (lambda (row) (equal (plist-get (car row) :user) "admin")) tabulated-list-entries))))
          (kafka-rpc-stop))))))

(provide 'kafka-admin-tests)
;;; kafka-admin-tests.el ends here
