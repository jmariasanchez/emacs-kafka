;;; kafka-security-tests.el --- Live security regressions -*- lexical-binding: t; -*-

(require 'ert)
(require 'kafka-tests)

(ert-deftest kafka-security-auth-source-scram ()
  "Resolve a real SCRAM credential exclusively from a temporary netrc."
  (skip-unless (and (getenv "KAFKA_EL_TEST_SECURE_SASL")
                    (executable-find kafka-bridge-program)))
  (let* ((file (make-temp-file "kafka-authinfo-"))
         (auth-sources (list file))
         (auth-source-do-cache nil)
         (kafka-clusters
          `(("security-netrc" :bootstrap ,(getenv "KAFKA_EL_TEST_SECURE_SASL")
             :security-protocol "SASL_PLAINTEXT" :sasl-mechanism "SCRAM-SHA-512"
             :sasl-username "admin" :timeout-ms 3000))))
    (unwind-protect
        (progn
          (set-file-modes file #o600)
          (with-temp-file file
            (insert "machine security-netrc login admin password admin-secret\n"))
          (auth-source-forget-all-cached)
          (should-not (plist-member (kafka--profile "security-netrc") :sasl-password))
          (should (kafka-ensure-connected "security-netrc"))
          (should (kafka-connected-p "security-netrc"))
          (let ((connections (kafka-rpc-request :kafka/connections nil)))
            (should-not (string-match-p "admin-secret" (prin1-to-string connections)))))
      (kafka-rpc-stop)
      (auth-source-forget-all-cached)
      (delete-file file))))

(ert-deftest kafka-security-wrong-password-fast ()
  (skip-unless (and (getenv "KAFKA_EL_TEST_SECURE_SASL")
                    (executable-find kafka-bridge-program)))
  (let ((kafka-clusters
         `(("security-invalid" :bootstrap ,(getenv "KAFKA_EL_TEST_SECURE_SASL")
            :security-protocol "SASL_PLAINTEXT" :sasl-mechanism "PLAIN"
            :sasl-username "admin" :sasl-password "invalid-sentinel" :timeout-ms 1500)))
        (start (float-time)))
    (unwind-protect
        (let ((error (should-error (kafka-ensure-connected "security-invalid"))))
          (should (< (- (float-time) start) 3.5))
          (should (string-match-p "authentication failed" (prin1-to-string error)))
          (should-not (kafka-connected-p "security-invalid")))
      (kafka-rpc-stop))))

(provide 'kafka-security-tests)
;;; kafka-security-tests.el ends here
