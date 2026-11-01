;;; kafka-registry-tests.el --- Schema Registry tests -*- lexical-binding: t; -*-
(require 'kafka-tests)
(require 'kafka-schema-registry)

(ert-deftest kafka-registry-auth-source-host ()
  (let ((kafka-clusters '(("registry" :schema-registry (:url "https://registry.example:8081/base" :basic-auth-user "alice"))))
        seen)
    (cl-letf (((symbol-function 'auth-source-search)
               (lambda (&rest args) (setq seen args) (list (list :secret (lambda () "password"))))))
      (let ((p (kafka-schema-registry--params "registry")))
        (should (equal (plist-get seen :host) "registry.example"))
        (should (equal (plist-get p :basicAuthPassword) "password"))
        (should (equal (plist-get p :basicAuthUser) "alice"))))))

(ert-deftest kafka-registry-non-wire-detail-no-network ()
  (let ((kafka-clusters '(("registry" :schema-registry (:url "http://localhost")))))
    (cl-letf (((symbol-function 'kafka-rpc-request) (lambda (&rest _) (ert-fail "unexpected network"))))
      (should-not (kafka-schema-registry-record-detail "registry" '(:value (:encoding "text" :data "plain"))))
      (should-not (kafka-schema-registry-record-detail "registry" '(:value nil))))))

(ert-deftest kafka-registry-read-only-register-before-network ()
  (let ((kafka-clusters '(("ro" :read-only t))))
    (cl-letf (((symbol-function 'kafka-rpc-request) (lambda (&rest _) (ert-fail "unexpected network"))))
      (with-temp-buffer
        (insert "\"string\"")
        (should-error (kafka-schema-registry-register "ro" "subject" "AVRO") :type 'user-error)))))

(ert-deftest kafka-registry-live-browser-and-record-detail ()
  (skip-unless (getenv "KAFKA_EL_TEST_REGISTRY"))
  (kafka-tests-with-cluster
    (setcdr (assoc "t-rw" kafka-clusters)
            (append (cdr (assoc "t-rw" kafka-clusters))
                    (list :schema-registry (list :url (getenv "KAFKA_EL_TEST_REGISTRY")))))
    (let* ((topic (kafka-tests-topic)) (subject (concat topic "-value")))
      (kafka-ensure-connected "t-rw")
      (kafka-rpc-request :kafka/topic/create (list :connectionId "t-rw" :topic topic :partitions 1 :replicationFactor 1))
      (unwind-protect
          (progn
            (with-temp-buffer
              (insert "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}")
              (kafka-schema-registry-register "t-rw" subject "AVRO")
              (should (eq (plist-get (kafka-schema-registry-check "t-rw" subject "AVRO") :is_compatible) t)))
            (kafka-schema-registry "t-rw")
            (with-current-buffer "*Kafka Schemas: t-rw*"
              (goto-char (point-min))
              (should (search-forward subject nil t))
              (beginning-of-line)
              (kafka-schema-registry-open))
            (with-current-buffer (format "*Kafka Schema Versions: t-rw/%s*" subject)
              (goto-char (point-min))
              (kafka-schema-registry-open))
            (with-current-buffer (format "*Kafka Schema: t-rw/%s/1*" subject)
              (should (derived-mode-p 'js-json-mode))
              (should (string-match-p "Test" (buffer-string))))
            (kafka-rpc-request :kafka/registry/produce
                              (list :connectionId "t-rw" :topic topic :value '(:name "ERT")))
            (let* ((result (kafka-rpc-request :kafka/records/fetch
                                            (list :connectionId "t-rw" :topic topic :startPosition '(:type "earliest") :limit 10)))
                   (record (seq-elt (plist-get result :records) 0))
                   (detail (kafka-schema-registry-record-detail "t-rw" record)))
              (should (string-match-p "schema id" detail))
              (should (string-match-p "ERT" detail))
              (with-temp-buffer
                (kafka-records-mode)
                (setq kafka-connection "t-rw")
                (cl-letf (((symbol-function 'kafka-records--at-point) (lambda () record)))
                  (kafka-records-show)))
              (with-current-buffer "*Kafka Record*" (should (string-match-p "Decoded AVRO" (buffer-string)))))
            (with-current-buffer "*Kafka Schemas: t-rw*"
              (goto-char (point-min))
              (search-forward subject)
              (beginning-of-line)
              (cl-letf (((symbol-function 'read-string) (lambda (&rest _) "wrong")))
                (should-error (kafka-schema-registry-delete) :type 'user-error))
              (cl-letf (((symbol-function 'read-string) (lambda (&rest _) subject)))
                (kafka-schema-registry-delete))))
        (ignore-errors (kafka-rpc-request :kafka/registry/delete (list :connectionId "t-rw" :subject subject :confirmName subject)))
        (ignore-errors (kafka-rpc-request :kafka/registry/delete (list :connectionId "t-rw" :subject subject :confirmName subject :permanent t)))
        (kafka-rpc-request :kafka/topic/delete (list :connectionId "t-rw" :topic topic :confirmName topic))))))

(provide 'kafka-registry-tests)
;;; kafka-registry-tests.el ends here
