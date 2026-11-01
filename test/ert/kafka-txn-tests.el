;;; kafka-txn-tests.el --- Producer and workflow tests -*- lexical-binding: t; -*-
;;; Commentary:
;; Integration cases use the plain broker and the real stdio bridge.
;;; Code:
(require 'kafka-tests)
(require 'kafka-transient)

(ert-deftest kafka-txn-lines-and-json ()
  (should (equal (kafka-producer--lines "a\n\nb\n") '("a" "" "b")))
  (should (equal (kafka-producer--line-record "key\tvalue\tmore") '(:key "key" :value "value\tmore")))
  (let ((kafka-producer-key-separator "="))
    (should (equal (kafka-producer--line-record "k=v=x") '(:key "k" :value "v=x"))))
  (let ((kafka-producer-key-separator nil))
    (should (equal (kafka-producer--line-record "k\tv") '(:value "k\tv"))))
  (should (equal (kafka-producer--file-record "raw") '(:value "raw")))
  (should (equal (kafka-producer--file-record "{\"id\":42}") '(:value "{\"id\":42}")))
  (should (equal (kafka-producer--file-record "{\"key\":\"k\",\"value\":\"v\",\"headers\":{\"source\":\"test\"}}")
                 '(:key "k" :value "v" :headers [(:key "source" :value "test")])))
  (should-error (kafka-producer--file-record "{bad json")))

(ert-deftest kafka-txn-batch-protection-before-network ()
  (let ((kafka-clusters '(("protected" :bootstrap "unused" :environment "production")))
        (kafka-protected-environments '("production"))
        (requests 0))
    (cl-letf (((symbol-function 'yes-or-no-p) (lambda (&rest _) nil))
              ((symbol-function 'kafka-ensure-connected) (lambda (&rest _) (cl-incf requests)))
              ((symbol-function 'kafka-rpc-request) (lambda (&rest _) (cl-incf requests))))
      (with-temp-buffer
        (insert "nope")
        (should-error (kafka-produce-region "protected" "t" (point-min) (point-max)) :type 'user-error))
      (should (= requests 0)))))

(ert-deftest kafka-txn-transient-menus ()
  (save-window-excursion
    (dolist (menu '(kafka-dispatch kafka-records-dispatch kafka-topics-dispatch kafka-groups-dispatch))
      (unwind-protect
          (progn
            (transient-setup menu)
            (should transient--suffixes)
            (dolist (suffix transient--suffixes)
              ;; Newer Transient retains hidden objects without evaluating
              ;; their inapt predicates.  Only visible suffixes are active.
              (when (and (or (null (oref suffix if)) (funcall (oref suffix if)))
                         (not (oref suffix inapt)))
                (should (fboundp (oref suffix command)))
                (should (commandp (oref suffix command))))))
        (transient-quit-one))))
  (should (eq (keymap-lookup kafka-records-mode-map "?") 'kafka-records-dispatch))
  (should (eq (keymap-lookup kafka-topics-mode-map "?") 'kafka-topics-dispatch))
  (should (eq (keymap-lookup kafka-groups-mode-map "?") 'kafka-groups-dispatch)))

(ert-deftest kafka-txn-transient-optional-command-activates ()
  (save-window-excursion
    (cl-letf (((symbol-function 'kafka-streams-apps) (lambda () (interactive))))
      (unwind-protect
          (progn
            (transient-setup 'kafka-dispatch)
            (let ((suffix (seq-find (lambda (s) (eq (oref s command) 'kafka-streams-apps))
                                    transient--suffixes)))
              (should suffix)
              (should-not (oref suffix inapt))
              (should (funcall (oref suffix if)))))
        (transient-quit-one)))))

(ert-deftest kafka-txn-diff-setup ()
  (let (captured buffers)
    (unwind-protect
        (with-temp-buffer
          (kafka-records-mode)
          (setq kafka-connection "test" kafka-records--topic "t"
                kafka-records--position '(:type "earliest"))
          (kafka-records--add [(:topic "t" :partition 0 :offset 0 :value (:encoding "text" :data "{\"id\":1}" :size 8))
                              (:topic "t" :partition 0 :offset 1 :value (:encoding "text" :data "{\"id\":2}" :size 8))])
          (kafka-records--render)
          (goto-char (point-min))
          (kafka-records-mark)
          (kafka-records-mark)
          (should (= (length kafka-workflows--marked) 2))
          (cl-letf (((symbol-function 'ediff-buffers)
                     (lambda (a b &rest _)
                       (setq buffers (list a b)
                             captured (mapcar (lambda (buf) (with-current-buffer buf (buffer-string))) buffers)))))
            (kafka-records-diff))
          (should (= (length captured) 2))
          (should (equal (car captured) (kafka-records--pretty "{\"id\":2}")))
          (should (equal (cadr captured) (kafka-records--pretty "{\"id\":1}"))))
      (mapc #'kill-buffer buffers))))

(defun kafka-txn-tests--topic ()
  (let ((topic (kafka-tests-topic)))
    (kafka-ensure-connected "t-rw")
    (kafka-rpc-request :kafka/topic/create
                       (list :connectionId "t-rw" :topic topic :partitions 1 :replicationFactor 1))
    topic))

(defun kafka-txn-tests--delete (topic)
  (kafka-rpc-request :kafka/topic/delete
                     (list :connectionId "t-rw" :topic topic :confirmName topic)))

(ert-deftest kafka-txn-it-produce-region-and-file ()
  (kafka-tests-with-cluster
    (let ((topic (kafka-txn-tests--topic))
          (file (make-temp-file "kafka-records-")))
      (unwind-protect
          (progn
            (with-temp-buffer
              (insert "key\tregion-one\nregion-two\n")
              (let ((result (kafka-produce-region "t-rw" topic (point-min) (point-max))))
                (should (= (length (plist-get result :results)) 2))))
            (with-temp-file file
              (insert "{\"key\":\"json-key\",\"value\":\"json-value\",\"headers\":{\"source\":\"file\"},\"partition\":0}\nraw-value\n{\"id\":42}\n{\"value\":null}\n{\"value\":\"\"}\n"))
            (should (eq (plist-get (kafka-produce-file "t-rw" topic file t) :committed) t))
            (let* ((result (kafka-rpc-request :kafka/records/fetch
                                             (list :connectionId "t-rw" :topic topic :limit 10
                                                   :startPosition '(:type "earliest") :isolationLevel "read_committed")))
                   (records (append (plist-get result :records) nil)))
              (should (equal (mapcar (lambda (r) (kafka-bytes-string (plist-get r :value))) records)
                             '("region-one" "region-two" "json-value" "raw-value" "{\"id\":42}" nil "")))
              (should (equal (kafka-bytes-string (plist-get (car records) :key)) "key"))
              (should (equal (plist-get (car (append (plist-get (nth 2 records) :headers) nil)) :key) "source"))))
        (delete-file file)
        (kafka-txn-tests--delete topic)))))

(ert-deftest kafka-txn-it-bookmark-round-trip ()
  (kafka-tests-with-cluster
    (let ((topic (kafka-txn-tests--topic))
          (bookmark-alist nil) (bookmark-save-flag nil)
          (bookmark-default-file (make-temp-name (expand-file-name "kafka-bookmarks-" temporary-file-directory)))
          (position '(:type "offset" :offset 0))
          buffer)
      (unwind-protect
          (progn
            (setq buffer (kafka-records "t-rw" topic '(0) position))
            (with-current-buffer buffer
              (setq kafka-records--filter "match")
              (bookmark-set "kafka-test"))
            (kill-buffer buffer)
            (bookmark-jump "kafka-test")
            (setq buffer (current-buffer))
            (should (equal kafka-records--topic topic))
            (should (equal kafka-records--position position))
            (should (equal kafka-records--partitions '(0)))
            (should (equal kafka-records--filter "match"))
            (should (equal kafka-connection "t-rw")))
        (when (buffer-live-p buffer) (kill-buffer buffer))
        (kafka-txn-tests--delete topic)))))

(ert-deftest kafka-txn-it-saved-query-and-header-search ()
  (kafka-tests-with-cluster
    (let* ((topic (kafka-txn-tests--topic))
           (kafka-saved-queries `(("saved" "t-rw" ,topic (:type "earliest") "match")))
           buffers)
      (unwind-protect
          (progn
            (kafka-producer--batch "t-rw" topic
                                   '((:key "match-key" :value "one")
                                     (:value "two" :headers [(:key "trace" :value "needle")])
                                     (:value "other")) nil)
            (let ((buffer (kafka-run-saved-query "saved")))
              (push buffer buffers)
              (with-current-buffer buffer
                (kafka-tests-wait (lambda () (= (length kafka-records--rows) 3)))
                (should (equal kafka-records--position '(:type "earliest")))
                (should (= (length tabulated-list-entries) 1))))
            (let ((buffer (kafka-records-search "t-rw" topic '(:type "earliest") 3 "needle")))
              (push buffer buffers)
              (with-current-buffer buffer
                (should (= (length kafka-records--rows) 1))
                (should (= (length tabulated-list-entries) 1))
                (should (equal (kafka-bytes-string (plist-get (car kafka-records--rows) :value)) "two")))))
        (mapc #'kill-buffer buffers)
        (kafka-txn-tests--delete topic)))))

(provide 'kafka-txn-tests)
;;; kafka-txn-tests.el ends here
