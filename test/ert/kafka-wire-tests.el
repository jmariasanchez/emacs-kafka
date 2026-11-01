;;; kafka-wire-tests.el --- Wire admin UI tests -*- lexical-binding: t; -*-
(require 'ert)
(require 'cl-lib)
(require 'kafka-tests)
(require 'kafka-cluster-admin)

(ert-deftest kafka-wire-plan-parser ()
  (with-temp-buffer
    (insert "# comment\n0: 2, 3\n1: cancel\n")
    (should (equal (kafka-reassignment--parse)
                   [(:partition 0 :replicas [2 3]) (:partition 1 :replicas nil)]))))

(ert-deftest kafka-wire-plan-rejects-invalid ()
  (dolist (plan '("" "0: 1,1" "0: 1\n0: 2" "-1: 2" "0: (shell-command x)" "0: 1,"))
    (with-temp-buffer
      (insert plan)
      (should-error (kafka-reassignment--parse) :type 'user-error))))

(ert-deftest kafka-wire-submit-preview-before-confirmation ()
  (let (calls)
    (with-temp-buffer
      (kafka-reassignment-mode)
      (setq kafka-connection "test" kafka-reassignment--topic "topic")
      (insert "0: 2,3\n")
      (cl-letf (((symbol-function 'kafka-reassignment--preview)
                 (lambda (_parts) (push 'preview calls)))
                ((symbol-function 'kafka-confirm-exact)
                 (lambda (_name _what topic) (push 'confirm calls) topic))
                ((symbol-function 'kafka-admin--request)
                 (lambda (_method &rest params)
                   (should (equal (plist-get params :confirmName) "topic"))
                   (push 'submit calls))))
        (kafka-reassignment-submit)
        (should (equal (reverse calls) '(preview confirm submit)))))))

(ert-deftest kafka-wire-submit-cancel-never-writes ()
  (with-temp-buffer
    (kafka-reassignment-mode)
    (setq kafka-connection "test" kafka-reassignment--topic "topic")
    (insert "0: 2\n")
    (cl-letf (((symbol-function 'kafka-reassignment--preview) #'ignore)
              ((symbol-function 'kafka-confirm-exact) (lambda (&rest _) (user-error "Cancelled")))
              ((symbol-function 'kafka-admin--request) (lambda (&rest _) (ert-fail "Unexpected write"))))
      (should-error (kafka-reassignment-submit) :type 'user-error))))

(ert-deftest kafka-wire-quota-label ()
  (should (equal (kafka-quota--label '((:entityType "user" :entityName "alice")
                                     (:entityType "client-id" :entityName nil)))
                 "user=alice, client-id=<default>")))

(ert-deftest kafka-wire-it-admin-views-and-plan ()
  (kafka-tests-with-cluster
    (let ((topic (kafka-tests-topic)))
      (kafka-ensure-connected "t-rw")
      (kafka-rpc-request :kafka/topic/create (list :connectionId "t-rw" :topic topic :partitions 1 :replicationFactor 1))
      (unwind-protect
          (progn
            (kafka-rpc-request :kafka/produce (list :connectionId "t-rw" :topic topic :value "wire-ert"))
            (dolist (command '(kafka-reassignments kafka-log-dirs kafka-quotas kafka-share-groups))
              (funcall command "t-rw")
              (with-current-buffer (window-buffer (selected-window))
                (should (derived-mode-p 'kafka-cluster-admin-mode))
                (should (equal kafka-connection "t-rw"))))
            (kafka-reassignment-edit "t-rw" topic)
            (with-current-buffer (format "*Kafka Replica Plan: t-rw/%s*" topic)
              (should (derived-mode-p 'kafka-reassignment-mode))
              (should (= (length (kafka-reassignment--parse)) 1))
              (let ((preview (kafka-reassignment-preview)))
                (should (equal (plist-get (elt (plist-get preview :changes) 0) :topic) topic)))
              (cl-letf (((symbol-function 'read-string) (lambda (&rest _) topic)))
                (kafka-reassignment-submit)))
            (kafka-ensure-connected "t-ro")
            (should-error
             (kafka-rpc-request :kafka/reassignments/alter
                                (list :connectionId "t-ro" :topic topic :confirmName topic
                                      :partitions [(:partition 0 :replicas [1])]))
             :type 'kafka-error))
        (kafka-rpc-request :kafka/topic/delete (list :connectionId "t-rw" :topic topic :confirmName topic))))))

(ert-deftest kafka-wire-it-quota-edit-and-delete ()
  (kafka-tests-with-cluster
    (let ((name (concat "wire-ert-" (kafka-tests-topic))))
      (kafka-quotas "t-rw")
      (with-current-buffer "*Kafka quotas: t-rw*"
        (unwind-protect
            (progn
              (kafka-quota-set "client-id" name "consumer_byte_rate" 987654)
              (kafka-tests-wait
               (lambda ()
                 (revert-buffer)
                 (goto-char (point-min))
                 (search-forward name nil t)))
              (beginning-of-line)
              (should (equal (plist-get (tabulated-list-get-id) :key) "consumer_byte_rate"))
              (cl-letf (((symbol-function 'read-string) (lambda (&rest _) name)))
                (kafka-quota-delete))
              (kafka-tests-wait
               (lambda ()
                 (revert-buffer)
                 (goto-char (point-min))
                 (not (search-forward name nil t)))))
          (kafka-admin--request :kafka/quotas/alter
                               :entity (vector (list :entityType "client-id" :entityName name))
                               :ops [(:key "consumer_byte_rate" :value nil)] :confirmName name))))))

(provide 'kafka-wire-tests)
;;; kafka-wire-tests.el ends here
