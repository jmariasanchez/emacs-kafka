;;; kafka-connect-tests.el --- Connect tests -*- lexical-binding: t; -*-
(require 'kafka-tests)
(require 'kafka-connect)

(ert-deftest kafka-connect-row-counts ()
  (should (equal (cadr (kafka-connect--row "test"
                      '(:status (:type "source" :connector (:state "RUNNING" :worker_id "host:8083")
                                  :tasks ((:state "RUNNING") (:state "FAILED"))))))
                 ["test" "source" "RUNNING" "1/1" "host:8083"])))

(ert-deftest kafka-connect-auth-source ()
  (let ((kafka-clusters '(("c" :connect (:url "https://connect.example" :basic-auth-user "alice")))))
    (cl-letf (((symbol-function 'kafka-ensure-connected) #'ignore)
              ((symbol-function 'auth-source-search)
               (lambda (&rest args)
                 (should (equal (plist-get args :host) "connect.example"))
                 '((:secret (lambda () "secret")))))
              ((symbol-function 'kafka-rpc-request)
               (lambda (method params)
                 (should (eq method :kafka/connect/configure))
                 (should (equal (plist-get params :basicAuthPassword) "secret")))))
      (kafka-connect--configure "c"))))

(ert-deftest kafka-connect-invalid-config-never-upserts ()
  (with-temp-buffer
    (kafka-connect-config-mode)
    (setq kafka-connection "c" kafka-connect--name "test")
    (insert "{\"connector.class\":\"test\"}")
    (cl-letf (((symbol-function 'kafka-confirm-write) #'ignore)
              ((symbol-function 'kafka-connect--show) #'ignore)
              ((symbol-function 'kafka-connect--request)
               (lambda (action &rest _)
                 (should (equal action "validate"))
                 '(:error_count 1))))
      (should-error (kafka-connect-save) :type 'user-error))))

(ert-deftest kafka-connect-read-only-before-validation ()
  (let ((kafka-clusters '(("ro" :read-only t))))
    (with-temp-buffer
      (kafka-connect-config-mode)
      (setq kafka-connection "ro")
      (cl-letf (((symbol-function 'kafka-connect--request)
                 (lambda (&rest _) (ert-fail "Network request on read-only profile"))))
        (should-error (kafka-connect-save) :type 'user-error)))))

(ert-deftest kafka-connect-it-real-worker ()
  (skip-unless (getenv "KAFKA_EL_TEST_CONNECT"))
  (kafka-tests-with-cluster
    (setcdr (assoc "t-rw" kafka-clusters)
            (append (cdr (assoc "t-rw" kafka-clusters))
                    (list :connect (list :url (getenv "KAFKA_EL_TEST_CONNECT")))))
    (kafka-connectors "t-rw")
    (with-current-buffer "*Kafka Connect: t-rw*"
      (should (derived-mode-p 'kafka-connectors-mode))
      (should (plist-get (kafka-connect--request "info") :version))
      (should (seq-some (lambda (p) (equal (plist-get p :class)
                                          "org.apache.kafka.connect.file.FileStreamSourceConnector"))
                        (kafka-connect--request "plugins"))))
    (kafka-streams-apps "t-rw")
    (with-current-buffer "*Kafka Streams: t-rw*"
      (should (derived-mode-p 'kafka-streams-mode)))))

(ert-deftest kafka-connect-it-edit-validate-upsert ()
  (skip-unless (getenv "KAFKA_EL_TEST_CONNECT"))
  (kafka-tests-with-cluster
    (let ((name (concat "kel-connect-" (kafka-tests-topic)))
          (file (make-temp-file "kel-connect-ert-")))
      (setcdr (assoc "t-rw" kafka-clusters)
              (append (cdr (assoc "t-rw" kafka-clusters))
                      (list :connect (list :url (getenv "KAFKA_EL_TEST_CONNECT")))))
      (kafka-connect--configure "t-rw")
      (unwind-protect
          (progn
            (kafka-connect--edit "t-rw" name
                                 (list :name name :connector.class "org.apache.kafka.connect.file.FileStreamSourceConnector"
                                       :tasks.max "1" :file file :topic name))
            (with-current-buffer (format "*Kafka Connector: t-rw/%s*" name)
              (goto-char (point-max))
              (kafka-connect-save)
              (should-not (buffer-modified-p)))
            (kafka-connectors "t-rw")
            (with-current-buffer "*Kafka Connect: t-rw*"
              (kafka-tests-wait
               (lambda () (revert-buffer) (assoc name tabulated-list-entries)))
              (should (assoc name tabulated-list-entries))))
        (kafka-rpc-request :kafka/connect/delete
                           (list :connectionId "t-rw" :connector name :confirmName name))
        (delete-file file)))))
(provide 'kafka-connect-tests)
;;; kafka-connect-tests.el ends here
