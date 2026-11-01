;;; kafka-tests.el --- ERT tests for kafka.el  -*- lexical-binding: t; -*-

;; Unit tests run anywhere.  Integration tests need:
;;   KAFKA_EL_BRIDGE=/path/to/kafka-bridge
;;   KAFKA_EL_TEST_BOOTSTRAP=127.0.0.1:19092

;;; Code:

(require 'ert)
(require 'kafka)

(defvar kafka-tests-bootstrap (getenv "KAFKA_EL_TEST_BOOTSTRAP"))

;;;; Unit tests

(ert-deftest kafka-test-clean-params ()
  (should (equal (kafka-rpc-clean-params '(:a 1 :b nil :c :json-false))
                 '(:a 1 :c :json-false))))

(ert-deftest kafka-test-bytes-display ()
  (should (equal (substring-no-properties (kafka-bytes-display nil)) "null"))
  (should (equal (kafka-bytes-display '(:encoding "text" :data "a\nb" :size 3)) "a b"))
  (should (string-match-p "binary 3 bytes"
                          (kafka-bytes-display '(:encoding "base64" :data "/wBB" :size 3))))
  (should (equal (kafka-bytes-string '(:encoding "base64" :data "/wBB")) "\377\0A"))
  (should (equal (kafka-bytes-display '(:encoding "text" :data "abcdef" :size 6) 3) "abc…")))

(ert-deftest kafka-test-connect-params-and-secrets ()
  (let ((kafka-clusters '(("p" :bootstrap "h1:9093,h2:9093" :read-only t
                           :security-protocol "SASL_SSL" :sasl-mechanism "PLAIN"
                           :sasl-username "u" :environment "production"
                           :properties (("client.rack" . "r1")))))
        (auth-sources nil))
    (cl-letf (((symbol-function 'auth-source-search)
               (lambda (&rest spec)
                 (when (and (equal (plist-get spec :host) "p") (equal (plist-get spec :user) "u"))
                   (list (list :secret (lambda () "s3cret")))))))
      (let ((p (kafka--connect-params "p")))
        (should (equal (plist-get p :bootstrapServers) "h1:9093,h2:9093"))
        (should (eq (plist-get p :readOnly) t))
        (should (equal (plist-get p :saslPassword) "s3cret"))
        (should (equal (plist-get p :securityProtocol) "SASL_SSL"))
        (should (equal (plist-get (plist-get p :properties) :client.rack) "r1"))))
    (should (kafka-protected-p "p"))
    (should-error (kafka-confirm-write "p" "x") :type 'user-error)))

(ert-deftest kafka-test-confirm-exact ()
  (let ((kafka-clusters '(("d" :bootstrap "x"))))
    (cl-letf (((symbol-function 'read-string) (lambda (&rest _) "wrong")))
      (should-error (kafka-confirm-exact "d" "Delete" "orders") :type 'user-error))
    (cl-letf (((symbol-function 'read-string) (lambda (&rest _) "orders")))
      (should (equal (kafka-confirm-exact "d" "Delete" "orders") "orders")))))

(ert-deftest kafka-test-producer-parse ()
  (with-temp-buffer
    (insert "Topic: orders\nPartition: 2\nKey: k1\nHeader: a=1\nHeader: b=x=y\n"
            kafka-producer-separator "\n{\"id\": 1}\nline2")
    (let ((p (kafka-producer-parse)))
      (should (equal (plist-get p :topic) "orders"))
      (should (equal (plist-get p :partition) 2))
      (should (equal (plist-get p :key) "k1"))
      (should (equal (plist-get p :value) "{\"id\": 1}\nline2"))
      (should (equal (plist-get p :headers) [(:key "a" :value "1") (:key "b" :value "x=y")]))))
  (with-temp-buffer
    (insert "Topic: t\nPartition: auto\nKey: \nTombstone: yes\n" kafka-producer-separator "\n")
    (let ((p (kafka-producer-parse)))
      (should (null (plist-get p :partition)))
      (should (null (plist-get p :key)))
      (should (null (plist-get p :value)))))
  (with-temp-buffer
    (insert "Topic: t\nBogus: 1\n" kafka-producer-separator "\n")
    (should-error (kafka-producer-parse) :type 'user-error))
  (with-temp-buffer
    (insert "Partition: 1\n" kafka-producer-separator "\n")
    (should-error (kafka-producer-parse) :type 'user-error)))

(ert-deftest kafka-test-parse-time ()
  (let ((now (* 1000 (float-time))))
    (should (< (abs (- (kafka-records--parse-time "-1h") (- now 3600000))) 5000))
    (should (> (kafka-records--parse-time "2026-01-01 00:00") 1700000000000))))

(ert-deftest kafka-test-hexdump ()
  (should (string-match-p "^00000000  ff 00 41 +\\.\\.A$"
                          (string-trim-right (kafka-records--hexdump "\377\0A")))))

(ert-deftest kafka-test-records-row-cap ()
  (with-temp-buffer
    (kafka-records-mode)
    (let ((kafka-records-max-rows 3))
      (kafka-records--add (vconcat (mapcar (lambda (i) (list :offset i)) (number-sequence 0 4))))
      (should (= (length kafka-records--rows) 3))
      (should (= kafka-records--dropped 2))
      (should (= (plist-get (car kafka-records--rows) :offset) 4)))))

(ert-deftest kafka-test-missing-bridge ()
  (let ((kafka-bridge-program "/nonexistent/kafka-bridge")
        (kafka-rpc--connection nil))
    (should-error (kafka-rpc-connection) :type 'user-error)))

;;;; Integration tests

(defmacro kafka-tests-with-cluster (&rest body)
  "Run BODY with a fresh bridge and profiles `t-rw' / `t-ro'."
  (declare (indent 0))
  `(progn
     (skip-unless (and kafka-tests-bootstrap (executable-find kafka-bridge-program)))
     (let ((kafka-clusters `(("t-rw" :bootstrap ,kafka-tests-bootstrap)
                             ("t-ro" :bootstrap ,kafka-tests-bootstrap :read-only t)))
           (kafka-protected-environments nil))
       (unwind-protect (progn ,@body)
         (kafka-rpc-stop)))))

(defun kafka-tests-wait (pred &optional secs)
  "Process events until PRED is non-nil, or fail after SECS."
  (let ((end (+ (float-time) (or secs 20))))
    (while (and (not (funcall pred)) (< (float-time) end))
      (accept-process-output nil 0.05))
    (should (funcall pred))))

(defun kafka-tests-topic ()
  (format "kel-ert-%d" (random 1000000000)))

(ert-deftest kafka-it-explorer-workflow ()
  (kafka-tests-with-cluster
    (let ((topic (kafka-tests-topic)))
      ;; Cluster list shows both profiles; connecting marks it.
      (kafka)
      (with-current-buffer "*Kafka Clusters*"
        (goto-char (point-min))
        (should (search-forward "t-rw" nil t))
        (beginning-of-line)
        (kafka-clusters-connect)
        (should (kafka-connected-p "t-rw")))
      ;; Read-only refuses create in Emacs before reaching the bridge.
      (should-error (kafka-topic-create "t-ro" topic 1 1 nil) :type 'user-error)
      (kafka-topic-create "t-rw" topic 3 1 '(:retention.ms "7200000"))
      ;; Topics list.
      (kafka-tests-wait
       (lambda ()
         (kafka-topics "t-rw")
         (with-current-buffer "*Kafka Topics: t-rw*"
           (goto-char (point-min))
           (search-forward topic nil t))))
      ;; Produce via the producer buffer.
      (with-current-buffer (kafka-produce "t-rw" topic 0)
      (goto-char (point-max))
      (insert "{\"hello\": \"world\"}")
      (save-excursion
        (goto-char (point-min))
        (re-search-forward "^Key: ")
        (insert "k1")
        (forward-line 1)
        (insert "Header: src=ert\n"))
      (let ((d (kafka-producer--send nil)))
        (should (= (plist-get d :partition) 0))
        (should (= (plist-get d :offset) 0))))
      ;; Read-only producer refused.
      (should-error (kafka-produce "t-ro" topic) :type 'user-error)
      ;; Topic detail.
      (kafka-topic "t-rw" topic)
      (with-current-buffer (format "*Kafka Topic: t-rw/%s*" topic)
        (should (= (length tabulated-list-entries) 3))
        (should (= (plist-get kafka-topic--detail :totalMessages) 1)))
      ;; Config view shows our override.
      (kafka-config-show "t-rw" 'topic topic)
      (with-current-buffer (format "*Kafka Config: t-rw topic %s*" topic)
        (should (equal (aref (cadr (assoc "retention.ms" tabulated-list-entries)) 1) "7200000")))
      ;; Records buffer: async fetch renders the record with key/header.
      (kafka-records "t-ro" topic nil '(:type "earliest"))
      (let ((buf (format "*Kafka Records: t-ro/%s*" topic)))
        (kafka-tests-wait (lambda () (with-current-buffer buf (= (length kafka-records--rows) 1))))
        (with-current-buffer buf
          (let ((r (car kafka-records--rows)))
            (should (equal (plist-get (plist-get r :key) :data) "k1"))
            (should (equal (plist-get (aref (plist-get r :headers) 0) :key) "src")))
          ;; Detail view pretty-prints JSON.
          (goto-char (point-min))
          (while (and (not (tabulated-list-get-id)) (not (eobp))) (forward-line 1))
          (kafka-records-show)
          (with-current-buffer "*Kafka Record*"
            (should (string-match-p "\"hello\": \"world\"" (buffer-string))))
          ;; Live tail: start at latest, produce, see it arrive.  On Emacs 29
          ;; (jsonrpc 1.0.16) this fails without `kafka-rpc--drain', because
          ;; the batch arrives in the same chunk as the produce reply.
          (setq kafka-records--position '(:type "latest"))
          (kafka-records-live)
          (should kafka-records--sub)
          (kafka-tests-wait (lambda () (equal kafka-records--state "active")))
          (kafka-rpc-request :kafka/produce (list :connectionId "t-rw" :topic topic :partition 1
                                                  :key "k2" :value "live!"))
          (kafka-tests-wait (lambda () (seq-some (lambda (r) (equal (plist-get (plist-get r :value) :data) "live!"))
                                                 kafka-records--rows)))
          (kafka-records-stop)
          (should-not kafka-records--sub)))
      ;; Killing a streaming buffer stops its subscription.
      (let ((buf (kafka-records "t-ro" topic nil '(:type "latest"))) sid)
        (with-current-buffer buf (kafka-records-live) (setq sid kafka-records--sub))
        (kill-buffer buf)
        (should-not (gethash sid kafka-records--subs))
        (kafka-tests-wait
         (lambda () (condition-case nil
                        (progn (kafka-rpc-request :kafka/consume/credit (list :subscriptionId sid)) nil)
                      (kafka-error t)))))
      ;; Groups list works and hides inspection groups.
      (kafka-groups "t-ro")
      (with-current-buffer "*Kafka Groups: t-ro*"
        (should-not (seq-some (lambda (e) (string-prefix-p "kafka.el-inspect" (car e)))
                              tabulated-list-entries)))
      ;; Delete requires exact name.
      (cl-letf (((symbol-function 'read-string) (lambda (&rest _) "nope")))
        (should-error (kafka-topic-delete "t-rw" topic) :type 'user-error))
      (cl-letf (((symbol-function 'read-string) (lambda (&rest _) topic)))
        (kafka-topic-delete "t-rw" topic)))))

(ert-deftest kafka-it-bridge-death-recovers ()
  (kafka-tests-with-cluster
    (kafka-ensure-connected "t-rw")
    (should (kafka-connected-p "t-rw"))
    (delete-process (jsonrpc--process kafka-rpc--connection))
    (kafka-tests-wait (lambda () (not (kafka-rpc-live-p))) 5)
    (should-not (kafka-connected-p "t-rw"))
    ;; Next use restarts the bridge and reconnects transparently.
    (should (kafka-ensure-connected "t-rw"))
    (should (kafka-rpc-live-p))))

(ert-deftest kafka-it-error-surfaces ()
  (kafka-tests-with-cluster
    (kafka-ensure-connected "t-rw")
    (let ((err (should-error (kafka-rpc-request :kafka/topic/describe
                                                (list :connectionId "t-rw" :topic "does-not-exist-xyz"))
                             :type 'kafka-error)))
      (should (stringp (cadr err))))
    (let ((kafka-clusters '(("bad" :bootstrap "127.0.0.1:1" :timeout-ms 500))))
      (should-error (kafka-ensure-connected "bad") :type 'kafka-error))))

(provide 'kafka-tests)
;;; kafka-tests.el ends here
